import os
import pandas as pd
from zipfile import ZipFile

def wazo_presale(filename, inventory_file, fileBOM=None, threshold=2):
    df = pd.read_excel(filename, header=0,
            names = ["OrderID",   # 0
                "Destination",      # 1
                "ProductID",        #2
                "Desc",             #3
                "Quantity",         #4
                "PreorderDateOnline",
                "EstimateDate",     #6
                "ContainerNum",
                "UnitReceived",
                "LifeCycle"],
            parse_dates=[6])

    df_inventory = pd.read_excel(inventory_file, header=0)
    df_inventory.rename(columns = {df_inventory.columns[0]:'ProductID'}, inplace = True)

    print(df_inventory.head(5))

    df_inventory['TotalATS'] = df_inventory.iloc[:, 2:].sum(axis=1)
    print(df_inventory.query('TotalATS > 0'))

    df_inventory.query('TotalATS <= @threshold', inplace=True)

    df.sort_values(['ProductID', 'EstimateDate'], ascending=[True, True], inplace=True)

    # find unique productIDs
    product_list = df_inventory['ProductID'].unique()
    print(f'total # of product need to locate orders on the sea is {len(product_list)}')

    orders_untouched = df.query('ProductID in @product_list and UnitReceived <= 0')
    presale_orders = orders_untouched.groupby(['ProductID']).first().reset_index()
    # change the columns order
    presale_cols = presale_orders.columns.tolist()
    presale_cols = presale_cols[1:3] + [presale_cols[0]] + presale_cols[3:]
    presale_orders = presale_orders[presale_cols]


    has_presale = len(presale_orders)
    print(f'# of presale order: {has_presale}')

    file_path = os.path.dirname(filename)
    if has_presale:
        presale_orders.to_csv(
            os.path.join(file_path, 'presale_orders.csv'), 
            index = False)

    # those SKU not in presale_order will be in repl_order
    product_list = presale_orders['ProductID'].unique()
    repl_orders = df_inventory.query('ProductID not in @product_list and TotalATS <= @threshold')
    has_repl = len(repl_orders)
    if has_repl:
        repl_orders.to_csv(
            os.path.join(file_path, 'replenish_orders.csv'), 
            index = False)

    # handle master/parent BOM
    # first read the master BOM information from a dedicate file named "ProductBillOfMaterials.csv"

    has_bom = False
    if fileBOM:
        has_bom = True
        print(f'fileBOM = {fileBOM}')
        dfBOM = pd.read_excel(fileBOM, header=0)
        dfBOM.rename(columns = {
            dfBOM.columns[0]:'ParentID',
            dfBOM.columns[1]:'ParentDesc',
            dfBOM.columns[3]:'ProductID'}, inplace = True)
        '''
        dfBOM = pd.read_excel(fileBOM, header=0,
            names = ["ParentID",
                "ParentDesc",
                "fillerC",
                "ProductID",
                "CompDesc",
                "fillerF",
                "fillG",
                "fillH",
                "fillI"])
        '''
        dfBOM = dfBOM[['ParentID', 'ParentDesc', 'ProductID']]
        dfBOM['ParentID'].ffill(inplace=True)
        dfBOM['ParentDesc'].ffill(inplace=True)

        dfBOM.query('ProductID in @product_list', inplace=True)
        print(dfBOM.head(5))
        print(repl_orders.head(5))
        print(presale_orders.head(5))

        dfBOM = dfBOM.join(presale_orders.append(repl_orders).set_index('ProductID'), on = 'ProductID')
        has_bom = len(dfBOM)
        if has_bom:
            dfBOM.to_csv(
                os.path.join(file_path, 'presaleBOM.csv'), 
                index = False)

    return_files = []
    return_files.append(
        os.path.join(file_path, 'presale_orders.csv') if has_presale else None)
    return_files.append(
        os.path.join(file_path, 'replenish_orders.csv') if has_repl else None)
    return_files.append(
        os.path.join(file_path,'presaleBOM.csv') if has_bom else None)

    return return_files

def zip_files(files):
    s = sum(1 for x in files if x)
    if s == 1:  # return the single file without bothering adding to zip
        for x in files: 
            if x:
                return os.path.basename(x)

    file_path = os.path.dirname([x for x in files if x][0])
    zfile = os.path.join(file_path, 'presales.zip')
    with ZipFile(zfile, 'w') as wazozip:
        for x in files:
            if x:
                wazozip.write(x, os.path.basename(x))
    return 'presales.zip'
