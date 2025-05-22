import os
import pyodbc
import polars as pl

import gurobipy as gp
from gurobipy import GRB

pl.Config(
    fmt_str_lengths = 1000, 
    tbl_width_chars = 1000,
    set_tbl_cols = 50,
    set_tbl_rows = 100
)


def get_file_path(keyword:str):
    excel_path = f'{os.getcwd()}'
    file = [i for i in os.listdir(excel_path) if f'{keyword}' in i][0]
    file_path = f'{excel_path}\\{file}'

    return file_path

def get_crate_data():

    file_path = get_file_path('MASTER')

    containers = (
        pl.read_excel(
            source = file_path, 
            sheet_name = 'Dates'
        )
    )

    crate_boms = (
        pl.read_excel(
            source = file_path,
            sheet_name = 'Crate BOMs',
            schema_overrides={'PO Sublot': pl.Utf8, 'UPDATED Sublot': pl.Utf8}
        )
        .rename({'crate no.': 'Crate Number', 'crate typ' : 'Crate Type', 'glass code' : 'Part Number', 'qty' : 'Crate Quantity', 'Single Column Sublot' : 'Sublot'})
        .select('Container Number', 'Crate Number', 'Crate Type', 'Part Number', 'Crate Quantity', 'PO Sublot', '# POs in Crate', 'Sublot')
        .with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
    )

    crates = crate_boms.select(pl.col('Crate Number', 'Crate Type', 'Container Number', 'Sublot')).unique()
    crate_boms = crate_boms.drop('Crate Type', 'Container Number')

    return containers, crates, crate_boms

def get_sublot_data():

    file_path = get_file_path('Takeoff')

    sublot_demand = (
        pl.read_excel(
            source = file_path, 
            sheet_name = 'Takeoff'
        )
        .filter(pl.col('GLASS HEIGHT (IN)') != 0)
        .select('Sublot', 'MODELED PART NUMBER')
        .with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
        .rename({'MODELED PART NUMBER' : 'Part Number'})
        .with_columns(
            pl.when((pl.col('Sublot') == '2.05') | (pl.col('Sublot') == '2.09'))
            .then(pl.lit('2.05&2.09'))
            .otherwise(pl.col('Sublot'))
            .alias('Sublot')
        )
        .with_columns(
            pl.when(pl.col('Sublot').str.len_chars() < 4)
            .then(pl.concat_str(pl.col('Sublot'), pl.lit('0')))
            .otherwise(pl.col('Sublot'))
            .alias('Sublot')
        )
        .group_by('Sublot', 'Part Number').len()
        .rename({'len' : 'BOM Quantity'})
    )

    return sublot_demand

# D_p = demand of part p in sublot s
def D_p(sublot_demand, sublot, part_number):

    quantity = (
        sublot_demand
        .filter(pl.col('Sublot') == sublot)
        .filter(pl.col('Part Number') == part_number)
        .select('Demand Quantity')
        .sum().item()
    )

    return quantity

# C_p = quantity of part p in crate c
def C_p(crate_boms, crate_number, part_number):
    
    quantity = (
        crate_boms
        .filter(pl.col('Crate Number') == crate_number)
        .filter(pl.col('Part Number') == part_number)
        .select('Crate Quantity')
        .sum().item()
    )

    return quantity

# E_p_s = already assigned # extras of part p in sublot s
def E_p_s(extras, sublot, part_number):
    
    quantity = (
        extras
        .filter(pl.col('Sublot') == sublot)
        .filter(pl.col('Part Number') == part_number)
        .select('NET')
        .sum().item()
    )

    return quantity

def linear_program(demand, crate_boms, extras):

    demand = demand.with_columns(pl.col(pl.String).str.replace_all(' ', ''))

    # parts in shipped GCs
    parts = crate_boms.select('Part Number').unique()

    sublots = demand.select('Sublot').unique()

    demand = (
        sublots
        .join(parts, how='cross')
        .join(demand, how='left', on=['Sublot', 'Part Number'])
        .fill_null(0)
        .with_columns(
            pl.col('NET')
            .mul(-1)
            .alias('Demand Quantity')
        )
        .select('Sublot', 'Part Number', 'Demand Quantity')
    )
    
    parts = parts.sort('Part Number')['Part Number'].to_list()
    sublots = sublots.sort('Sublot')['Sublot'].to_list()
    crates = crate_boms.select('Crate Number').unique().sort('Crate Number')['Crate Number'].to_list()

    m = gp.Model('model 1')

    x = m.addVars(crates, sublots, vtype=GRB.BINARY, name='x')
    y = m.addVars(parts, sublots, vtype=GRB.INTEGER, name='y')
    z = m.addVars(parts, sublots, vtype=GRB.INTEGER, name='z')

    m.update()

    i = 0
    for c in crates:
        i += 1

        ## each crate must be assigned a sublot
        m.addConstr(gp.quicksum(x[c, s] for s in sublots) == 1, name=f'x_{i}')

    k = 0
    for s in sublots:
        k += 1
        j = 0
        for p in parts:
            j += 1

            ## y = quantity of part p in sublot s stack
            m.addConstr(y[p,s] == (E_p_s(extras, s, p) + gp.quicksum(x[c, s] * C_p(crate_boms,c,p) for c in crates)), name=f'y_{j}_{k}')

            ## z = max(0, y - demand) = quantity of extras of part p in sublot s stack
            ##    z >= 0
            ##    z >= y - demand of part p in sublot s
            m.addConstr(z[p,s] >= 0, name=f'z_0_{j}_{k}')
            m.addConstr(z[p,s] >= (y[p,s] - D_p(demand, s, p)) , name=f'z_d_{j}_{k}')

    m.setObjective(gp.quicksum(z[p,s] for p in parts for s in sublots), GRB.MINIMIZE)
    m.optimize()
    # print(m.display())

    answers = pl.DataFrame()
    for v in m.getVars():
        row = pl.DataFrame({'Name' : [v.VarName], 'Value': [v.X]})
        answers = pl.concat([answers, row])

    crate_selection = (
        answers
        .filter(pl.col('Name').str.starts_with('x'))
        .filter(pl.col('Value') == 1)
        .with_columns(pl.col('Name').str.strip_chars('x[]'))
        .select('Name')
        .to_series()
    )
    print('\n\ncrate-sublot combos!!!!')
    print(crate_selection)

    crate_assignments = (
        answers
        .filter(pl.col('Name').str.starts_with('x'))
        .filter(pl.col('Value') == 1)
        .with_columns(pl.col('Name').str.strip_chars('x[]'))
        .with_columns(
            pl.col('Name').str.split(',').list.first()
            .alias('Crate Number')
        )
        .with_columns(
            pl.col('Name').str.split(',').list.last()
            .alias('Sublot')
        )
        .select('Crate Number', 'Sublot')
        .sort('Crate Number')
    )
    print('\n\n\n\ncrate assignments!')
    print(crate_assignments)

    extras = (
        answers
        .filter(pl.col('Name').str.starts_with('z'))
        .filter(pl.col('Value') > 0)
        .with_columns(pl.col('Name').str.strip_chars('z[]'))
        .with_columns(
            pl.col('Name').str.split(',').list.first()
            .alias('Part Number')
        )
        .with_columns(
            pl.col('Name').str.split(',').list.last()
            .alias('Sublot')
        )
        .drop('Name')
        .with_columns(pl.col('Value').cast(pl.Int64))
    )
    print('\nextras')
    print(extras)


def main():

    containers, crates, crate_boms = get_crate_data()
    sublot_demand = get_sublot_data()
    sublot_crates = (
        crate_boms
        .group_by('Part Number', 'Sublot')
        .agg(pl.col('Crate Quantity').sum())
    )

    # NOTE filtered to show phase 2 only
    flat_demand_crates = (
        sublot_demand
        .join(sublot_crates, how='full', on=['Part Number', 'Sublot'])
        .with_columns(
            pl.coalesce(['Sublot', 'Sublot_right'])
            .alias('Sublot')
        )
        .with_columns(
            pl.coalesce(['Part Number', 'Part Number_right'])
            .alias('Part Number')
        )
        .drop(['Sublot_right', 'Part Number_right'])
        .fill_null(0)
        .with_columns(
            (pl.col('Crate Quantity') - pl.col('BOM Quantity'))
            .alias('NET')
        )
    )

    free_crates = (
        crate_boms
        .filter(pl.col('Sublot').is_null())
        .group_by('Crate Number', 'Part Number')
        .agg(pl.col('Crate Quantity').sum())
        .join(crates, on = 'Crate Number')
        .select('Container Number', 'Crate Number', 'Part Number', 'Crate Quantity')
        .sort('Container Number', 'Crate Number', 'Part Number')
    )

    sublots_shipped = (
        flat_demand_crates        
        .filter(pl.col('Sublot').is_not_null())
        .group_by('Sublot')
        .agg(pl.col('Crate Quantity').sum())
        .filter(pl.col('Crate Quantity') > 0)
        .select('Sublot')
        .sort('Sublot')
    )

    unfulfilled_demand = (
        flat_demand_crates
        .join(sublots_shipped, how = 'inner', on = 'Sublot')
        .filter(pl.col('NET') < 0)
        .sort('Sublot', 'Part Number')
    )

    different_project_material = (
        flat_demand_crates
        .filter(~pl.col('Part Number').str.starts_with('1116'))
    )

    extras = (
        flat_demand_crates
        .filter(pl.col('NET') > 0)
        .filter(pl.col('Part Number').str.starts_with('1116'))
        .sort('Sublot', 'Part Number')
    )

    print('\nfreee cratessss')
    print(free_crates)

    print('\nextras')
    print(extras)

    print('\nunfulfilled demand')
    print(unfulfilled_demand)

    print('\nnon-CHOP material')
    print(different_project_material)

    print(unfulfilled_demand.filter(pl.col('Part Number') == '1116GLZ02-10-0004'))

    if free_crates.is_empty():
        print('all crates assigned sublots!')
    else:
        linear_program(unfulfilled_demand, free_crates, extras)



### RUN IT ###
if __name__ == '__main__':
    main()