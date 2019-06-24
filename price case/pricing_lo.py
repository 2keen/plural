import csv
import pulp


problem = pulp.LpProblem('holidayPricing', pulp.LpMaximize)


class product:
    def __init__(self,num,sku,name,market,cost,price_2018,lowes_2018,elasticity,units_2018):
        self.num = num
        self.sku = sku
        self.name = name
        self.market = market
        self.cost = cost
        self.price_2018 = price_2018
        self.lowes_2018 = lowes_2018
        self.elasticity = elasticity
        self.units_2018 = units_2018
        self.price_2019 = self.set_price()

    def intercept(self):
        return self.units_2018 - (self.elasticity * self.price_2018)
    
    def sales_2018(self):
        return self.units_2018 * self.price_2018
    
    def cost_2018(self):
        return self.units_2018 * self.cost
        
    def profit_2018(self):
        return self.sales_2018() - self.cost_2018()
        
    def margin_2018(self):
        return self.profit_2018() / self.sales_2018()
        
    def set_price(self, price=1):
        self.price_2019 = price
    
    def units_2019(self):
        return (self.elasticity * self.price_2019) + self.intercept()
    
    def sales_2019(self):
        return self.units_2019() * self.price_2019
    
    def cost_2019(self):
        return self.units_2019() * self.cost
        
    def profit_2019(self):
        return self.sales_2019() - self.cost_2019()
        
    def margin_2019(self):
        return self.profit_2019() / self.sales_2019()   

    def units_vly(self):
        return self.units_2019() - self.units_2018
    
    def sales_vly(self):
        return self.sales_2019() - self.sales_2018()
    
    def cost_vly(self):
        return self.cost_2019() - self.cost_2018()
        
    def profit_vly(self):
        return self.profit_2019() - self.profit_2018()
        
    def margin_vly(self):
        return self.margin_2019() - self.margin_2018()        

############################################################################
def get_units(price_list):
    units = ''
    for pi, p in enumerate(price_list):
        products[pi].set_price(p)
        units.append(str(products[pi].units_2019()))
    return units


def get_margin(price_list):
    for pi, p in enumerate(price_list.values()):
        products[pi].set_price(p)
    sales = sum(p.sales_2019() for p in products)
    cost = sum(p.sales_2019() for p in products)
    return (sales - cost) / cost

###########################################################################


def load_products():
    products = []
    with open('pricing_lo.csv') as f: 
        reader = csv.reader(f)
        for row, data in enumerate(reader):
            if row == 0:
                continue
            products.append(
                product(num = row-1,
                    sku=data[0],
                    name = data[1],
                    market = data[2],
                    cost = int(data[3][1:]),
                    price_2018 = int(data[4][1:]),
                    lowes_2018 = int(data[5][1:]),
                    elasticity = float(data[6]),
                    units_2018 = int(data[8])            
                )
            )
    return products    


products = load_products()
    
# Decision variables
#pricing = pulp.LpVariable.dicts("pricing_2019", (p.num for p in products), 1, 200, cat="Continuous")
pricing = []
pricing2 = []
for p in products:
    v = pulp.LpVariable('pricing_{}'.format(p.sku), lowBound=1, upBound=200, cat='Continuous')
    pricing.append(v)
    for num in range(1,201):
        v = pulp.LpVariable('tangent_{0}_{1}'.format(p.sku,num), lowBound=0, upBound=1, cat='Integer')
        pricing2.append(v)


    

# Constraints
######################################################################
#  goal constraint
def goal_function(price_list):
    for pi, p in price_list.items():
        if pi == 0:
            goal = ((products[pi].elasticity * p) + products[pi].intercept())
        else:
            goal = goal + ((products[pi].elasticity * p) + products[pi].intercept())
    return goal

problem += goal_function(pricing), 'Maximize units'  # objective function
######################################################################
#  tangent constraints
def get_sales(binary_decision, x):
    v = binary_decision
    x1 = int(v.name[v.name.rfind('_') + 1:])
    if x1 != x:
        return 0
    sku = v.name[v.name.find('_')+1:v.name.rfind('_')]
    for p in products:
        if sku == p.sku:
            a = p.elasticity
            b = p.intercept()
            break
    return ((2 * a * x1) + b) * (x - (a * (x1*x1) + b))

for v in pricing2:
    for i, p in pricing:
        problem += v.name[v.name.find('_')+1:v.name.rfind('_')] == p.sku 
        problem += int(v.name[v.name.rfind('_') + 1:]) * v == 

######################################################################
#  margin constraint
def agg_margin(price_list):
    tot_sales = 0
    tot_vost = 0
    for i, var in price_list.items():
        pr = products[i]
        for t in pricing2:
            if get_sales(t, var) == 0:
                continue
            else:
                tot_sales += get_sales(t, var)
                tot_cost += (pr.elasticity * var + pr.intercept()) * pr.cost
    return (tot_sales - tot_cost) / tot_cost
 
 
problem += agg_margin(pricing) >= .78  
######################################################################
######################################################################
#  tangent constraints

######################################################################





for pi, p in enumerate(products):
    problem += pricing[pi] <= p.lowes_2018,     'lowes_limit_{0}'.format(p.sku)
    problem += pricing[pi] >= p.cost,               'no loss leader_{0}'.format(p.sku)
    



#problem.solve()
#for x in pricing.values():
#    print(x.varValue)
    


