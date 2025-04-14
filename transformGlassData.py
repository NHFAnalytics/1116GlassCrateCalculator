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
        .with_columns(
            pl.col('no')
            .str.split("Sublot ").list.last()
            .str.split(' G').list.first()
            .alias('PO Sublot')
        )
        .rename({'crate no.': 'Crate Number', 'crate typ' : 'Crate Type', 'glass code' : 'Part Number', 'qty' : 'Crate Quantity'})
        .select('Container Number', 'Crate Number', 'Crate Type', 'Part Number', 'Crate Quantity', 'PO Sublot', '# POs in Crate', 'UPDATED Sublot')
        .with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
        .with_columns(
            pl.when(pl.col('# POs in Crate') == 1)
            .then(pl.col('PO Sublot'))
            .when(~pl.col('UPDATED Sublot').is_null())
            .then(pl.col('UPDATED Sublot'))
            .otherwise(None)
            .alias('Sublot')
        )
    )

    crates = crate_boms.select(pl.col('Crate Number', 'Crate Type', 'Container Number', 'Sublot')).unique()

    crate_boms = crate_boms.drop('Crate Type', 'Container Number')
    
    print(crate_boms)
    print(crates)

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
        .rename({'MODELED PART NUMBER' : 'Part Number'})
        .with_columns(
            pl.when((pl.col('Sublot') == '2.05') | (pl.col('Sublot') == '2.09'))
            .then(pl.lit('2.05 & 2.09'))
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

    parts = sublot_demand.select('Part Number').unique()

    return parts, sublot_demand


def linear_program(demand, crate_boms):

    if crate_boms.is_empty():
        print('all crates have been assigned sublots')
    else:
        demand = demand.with_columns(pl.col('Sublot').str.replace_all(' ', ''))

        print('\n\n\n\n\nINSIDE DA LOOOOP\n\n')

        # parts in shipped GCs
        parts = crate_boms.select('Part Number').unique().sort('Part Number')['Part Number'].to_list()

        # filter demand for parts already shipped
        demand = (
            demand
            .filter(pl.col('Part Number').is_in(parts))
            .with_columns(
                pl.col('NET')
                .mul(-1)
                .alias('Demand Quantity')
            )
            .select('Sublot', 'Part Number', 'Demand Quantity')
        )

        sublots = demand.select('Sublot').unique().sort('Sublot')['Sublot'].to_list()
        crates = crate_boms.select('Crate Number').unique().sort('Crate Number')['Crate Number'].to_list()

        m = gp.Model('model 1')

        x = m.addVars(crates, sublots, vtype=GRB.BINARY, name='x')      
        z = m.addVars(parts, sublots, vtype=GRB.INTEGER, name='z')  

        i = 0
        for c in crates:
            i += 1
            m.addConstr(gp.quicksum(x[c, s] for s in sublots) == 1, name=f'c_{i}')

        obj = 0 #gp.LinExpr

        k = 0
        for s in sublots:
            k += 1

            j = 0
            for p in parts:
                j += 1
                sublot_demand = (
                    demand
                    .filter(pl.col('Sublot') == s)
                    .filter(pl.col('Part Number') == p)
                    .select('Demand Quantity')
                    .sum().item()
                )
                print(f'part {p} has ({sublot_demand}) demand in sublot {s}')


                temp_crates = crate_boms.select('Crate Number', 'Part Number', 'Crate Quantity').to_dicts()
                print(temp_crates)

                
                print(crate_boms)

                for c in crates:

                    print(temp_crates[c, p])


                m.addConstr(
                    z[p, s] == gp.max_(
                        (-sublot_demand
                            + gp.quicksum(
                                x[c, s] * 
                                (crate_boms
                                    .filter(pl.col('Part Number') == p)
                                    .filter(pl.col('Crate Number') == c)
                                    .select('Crate Quantity')
                                    .sum().item()
                                )
                                for c in crates
                            )
                        )
                        , constant=0
                    )
                    ,name=f'z_{j},{k}'
                )

        m.setObjective(obj, GRB.MINIMIZE)

        m.optimize()

        print(m.display())

        for v in m.getVars():
            
            #print crate-sublot combos
            if v.X == 1:
                print('%s' % (v.VarName))


def linear_program_OLD(demand, crate_boms):
    
    if crate_boms.is_empty():
        print('all crates have been assigned sublots')
    else:
        demand = demand.with_columns(pl.col('Sublot').str.replace_all(' ', ''))

        print('\n\n\n\n\nINSIDE DA LOOOOP\n\n')
        print('crate boms')
        print(crate_boms)
        print('all demand')
        print(demand)

        # parts in shipped GCs
        parts = crate_boms.select('Part Number').unique().sort('Part Number')

        # filter demand for parts already shipped
        demand = demand.filter(pl.col('Part Number').is_in(parts))
        demand = (
            demand.with_columns(
                pl.col('NET')
                .mul(-1)
                .alias('Demand Quantity')
            )
            .select('Sublot', 'Part Number', 'Demand Quantity')
        )

        sublots = demand.select('Sublot').unique().sort('Sublot')

        crates = crate_boms.select('Crate Number').unique().sort('Crate Number')

        print('filtered demand')
        print(demand)
        print('parts')
        print(parts)
        print('sublots')
        print(sublots)
        print('crates')
        print(crates)

        crates = crates['Crate Number'].to_list()
        sublots = sublots['Sublot'].to_list()
        parts = parts['Part Number'].to_list()

        print(crates)
        print(sublots)
        print(parts)


        m = gp.Model('model 1')

        x = m.addVars(crates, sublots, vtype=GRB.BINARY, name='x')        

        i = 0
        for c in crates:
            i += 1
            m.addConstr(gp.quicksum(x[c, s] for s in sublots) == 1, name=f'c_{i}')

        obj = 0 #gp.LinExpr

        i = 0
        for s in sublots:
            sublot_demand = (
                demand
                .filter(pl.col('Sublot') == s)
                .select('Demand Quantity')
                .sum().item()
            )

            print(f'total sublot {s} demand: {sublot_demand}')

           # obj += - sublot_demand


            for p in parts:
                i += 1
                part_obj = 0
                sublot_demand = (
                    demand
                    .filter(pl.col('Sublot') == s)
                    .filter(pl.col('Part Number') == p)
                    .select('Demand Quantity')
                    .sum().item()
                )

                print(f'sublot {s} has ({sublot_demand}) demand for {p}')

                for c in crates:

                    # check if part number p is in crate c
                    # if part number is in crate, then add to objective

                    crate_quantity = (
                        crate_boms
                        .filter(pl.col('Part Number') == p)
                        .filter(pl.col('Crate Number') == c)
                        .select('Crate Quantity')
                        .sum().item()
                    )

                    if crate_quantity > 0:
                        part_obj += crate_quantity * x[c, s]
                        print(f'adding part {p} in crate {c}')
                    else:
                        print('    not in GC')
                
            #### z[]
              
                try:
                    part_obj != 0
                except Exception as error:
                    ''
                   # m.addConstr(z == max((part_obj - sublot_demand), 0), name=f'd_{i}')             



        m.setObjective(obj, GRB.MINIMIZE)

        m.optimize()

        print(m.display())

        for v in m.getVars():
            
            #print crate-sublot combos
            if v.X == 1:
                print('%s' % (v.VarName))

def main():

    containers, crates, crate_boms = get_crate_data()

    parts, sublot_demand = get_sublot_data()

    clean_crate_boms = crate_boms.group_by('Part Number', 'Sublot').agg(pl.col('Crate Quantity').sum())

   # sublot_demand = sublot_demand.filter(pl.col('Sublot').str.starts_with('2.')).filter(pl.col('Sublot') == '2.01')
   # clean_crate_boms = clean_crate_boms.filter(pl.col('Sublot') == '2.01')

    temp = (
        sublot_demand
        # NOTE only filtered to show phase 2
        .join(clean_crate_boms, how='full', on=['Part Number', 'Sublot'])
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

    sublots = (
        temp.group_by('Sublot')
        .agg(pl.col('Crate Quantity').sum())
        .filter(pl.col('Crate Quantity') > 0)
        .filter(pl.col('Sublot').is_not_null())
        .select('Sublot')
        .sort('Sublot')
    )

    unfulfilled_demand = (
        temp
        .join(sublots, how = 'inner', on = 'Sublot')
        .filter(pl.col('NET') < 0)
        .sort('Sublot', 'Part Number')
    )

    print('unfulfilled demand')
    print(unfulfilled_demand)
    print(f'unfulfilled demand sum: {unfulfilled_demand.sum().select('NET').item()}')

    print('extras')
    print(temp.filter((pl.col('NET') > 0) & (pl.col('Sublot').is_not_null())))

    #print('missing')
    #print(temp.join().filter(pl.col('NET') < 0))
    print('freee cratessss')
    print(free_crates)


    linear_program(unfulfilled_demand, free_crates)



    # recommend sublots for mixed crates to go to
        # backfill in comparison to the MS dates?????

   # fulfill_demand = sublot_demand.join(crate_boms, on= ['Part Number', 'Sublot'], how="full")

  #  print(fulfill_demand.filter(pl.col('Sublot').str.starts_with('2.')))


### RUN IT ###
if __name__ == '__main__':
    main()