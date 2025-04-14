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
            sheet_name = 'Crate BOMs'
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
            pl.when(pl.col('# POs in Crate') == 1)
            .then(pl.col('PO Sublot'))
            .when(pl.col('UPDATED Sublot').str != '')
            .then(pl.col('UPDATED Sublot'))
            .otherwise(pl.lit(''))
            .alias('Sublot')
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

    print('free crates')
    print(free_crates)
    print(free_crates.sum())

    sublots = (
        temp.group_by('Sublot')
        .agg(pl.col('Crate Quantity').sum())
        .filter(pl.col('Crate Quantity') > 0)
        .filter(pl.col('Sublot').is_not_null())
        .select('Sublot')
        .sort('Sublot')
    )

    temp_2 = (
        temp
        .join(sublots, how = 'inner', on = 'Sublot')
        .filter(pl.col('NET') < 0)
        .sort('Sublot', 'Part Number')
    )

    print('unfulfilled demand')
    print(temp_2)
    print(temp_2.sum())

    print(sublot_demand.filter((pl.col('Part Number') == '1116GLT01-10-0001')).filter(pl.col('Sublot') == '2.02'))
    print(temp.filter(pl.col('Sublot') == '2.02').sort('Part Number'))

    print(crate_boms.filter(pl.col('Sublot') == '2.02').group_by('Crate Number', 'Part Number').agg(pl.col('Crate Quantity').sum()).sort('Crate Number', 'Part Number'))


    print('extras')
    print(temp.filter((pl.col('NET') > 0) & (pl.col('Sublot').is_not_null())))

    #print('missing')
    #print(temp.join().filter(pl.col('NET') < 0))

    ## TO DO
    # stack demand and crates
    # group by part number and sublot and quantities to get parts in sublot vs. auto-assigned crates
    # return list of extras and unfulfilled demand

    # recommend sublots for mixed crates to go to
        # backfill in comparison to the MS dates?????

   # fulfill_demand = sublot_demand.join(crate_boms, on= ['Part Number', 'Sublot'], how="full")

  #  print(fulfill_demand.filter(pl.col('Sublot').str.starts_with('2.')))


### RUN IT ###
if __name__ == '__main__':
    main()