import os
import pyodbc
import polars as pl


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
            .str.split(' ').list.first()
            .alias('PO Sublot')
        )
        .rename({'crate no.': 'Crate Number', 'crate typ' : 'Crate Type', 'glass code' : 'Part Number', 'qty' : 'Crate Quantity'})
        .select('Container Number', 'Crate Number', 'Crate Type', 'Part Number', 'Crate Quantity', 'PO Sublot', '# POs in Crate')
        .with_columns(
            pl.when(pl.col('# POs in Crate') == 1)
            .then(pl.col('PO Sublot'))
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
        .group_by('Sublot', 'Part Number').len()
        .rename({'len' : 'BOM Quantity'})
    )

    parts = sublot_demand.select('Part Number').unique()

    return parts, sublot_demand





def main():

    containers, crates, crate_boms = get_crate_data()

    parts, sublot_demand = get_sublot_data()

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