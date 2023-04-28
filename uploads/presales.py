import os
import pandas as pd
from zipfile import ZipFile

def wazo_presale(order_file, inv_file, rules_file, fileBOM=None, threshold=2):
    df_order = pd.read_excel(order_file, header=0, parse_dates=[9]).iloc[:,[0,9,10,12,14]]
    df_order.columns = ['OrderID', 'EstimateDate', 'ProductID', 'Quantity', 'UnitReceived']
    df_order.sort_values(['ProductID', 'EstimateDate'], ascending=[True, True], inplace=True)
            #names = ["OrderID",     # 0 APurchaseOrderWDetails_Sample.xlsx
            #    "Supplier",         # 1 B
            #    "Destination",      # 2 C
            #    "Event",            # 3 D
            #    "Container",        # 4 E
            #    "Planned Ex"        # 5 F
            #    "Acturual Ex"       # 6 G
            #    "Planned POD"       # 7 H
            #    "Planned Sail"      # 8 I
            #    "EstimateDate",     # 9 J
            #    "ProductID",        #10 K
            #    "Desc",             #11 L
            #    "Quantity",         #12 M
            #    "PCs Count",        #13 N
            #    "UnitReceived",     #14 O
            #    "CBM per unit",     #15 P
            #    "PreorderDateOnline"] #16 Q
    df_inv = pd.read_excel(inv_file, header=0)
    df_inv.rename(columns = {df_inv.columns[0]:'ProductID',
        df_inv.columns[1]:'Subcategory'}, inplace = True)

    df_rules = pd.read_excel(rules_file, header=0)
    df_rules.rename(columns = {df_rules.columns[0]:'Subcategory',
        df_rules.columns[1]:'PreorderThreshold'}, inplace = True)

    print(df_inv.head(5))
    print(df_rules.head(5))

    # join preoder rules with inventory files
    df_inv = pd.merge(df_inv, df_rules, how = "left", on = "Subcategory").fillna(threshold)
    print(df_inv.head(5))

    # check if any warehouse stock is less than the threshold
    df_inv = df_inv[(df_inv.iloc[:, 2:-1].sub(df_inv['PreorderThreshold'], axis=0) <= 0).any(axis=1)]
    print('after checking stock:', df_inv.head(5))


    # find unique productIDs
    product_list = df_inv['ProductID'].unique()
    print(f'total # of product need to locate orders on the sea is {len(product_list)}')

    orders_untouched = df_order.query('ProductID in @product_list and UnitReceived-Quantity <= 0')
    presale_orders = orders_untouched.groupby(['ProductID']).first().reset_index()
    # change the columns order
    presale_cols = presale_orders.columns.tolist()
    presale_cols = presale_cols[1:3] + [presale_cols[0]] + presale_cols[3:]
    presale_orders = pd.merge(presale_orders[presale_cols], df_inv, how='left', on='ProductID')



    # those SKU not in presale_order will be in repl_order
    product_list = presale_orders['ProductID'].unique()
    repl_orders = df_inv.query('ProductID not in @product_list')

    # handle master/parent BOM
    # first read the master BOM information from a dedicate file named "ProductBillOfMaterials.csv"

    has_bom = False
    if fileBOM:
        has_bom = True
        print(f'fileBOM = {fileBOM}')
        dfBOM = pd.read_excel(fileBOM, header=0)
        dfBOM.rename(columns = {
            dfBOM.columns[0]:'ParentID',
            dfBOM.columns[2]:'ProductID'}, inplace = True)
        dfBOM = dfBOM[['ParentID', 'ProductID']]
        #dfBOM['ParentID'].ffill(inplace=True)

        #dfBOM.query('ProductID in @product_list', inplace=True)
        #print(dfBOM.head(5))

        # associate parent product ID if an order itme is a component 
        presale_orders = pd.merge(presale_orders, dfBOM, how = "left", on = "ProductID")
        repl_orders = pd.merge(repl_orders, dfBOM, how = "left", on = "ProductID")


    file_path, return_files = os.path.dirname(inv_file), []
    if len(presale_orders):
        presale_orders.to_csv(
            os.path.join(file_path, 'presale_orders.csv'), 
            index = False)
        return_files.append(os.path.join(file_path, 'presale_orders.csv')) 
    if len(repl_orders):
        repl_orders.to_csv(
            os.path.join(file_path, 'replenish_orders.csv'), 
            index = False)
        return_files.append(os.path.join(file_path, 'replenish_orders.csv'))

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
