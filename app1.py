import os
from flask import Flask, render_template, flash, request, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
#from numpy.lib.shape_base import expand_dims
import pandas as pd
import sys
import openpyxl

def presale(filename):
    df = pd.read_csv(filename, header=0,
        names = ["OrderDate",
                "OrderID",
                "ProductID",
                "Desc",
                "Quantity",
                "ContainerNum",
                "UnitReceived",
                "EstimateDate",
                "WZ0002",
                "WZ0001"],
        parse_dates=[0, 7])
    df.query('WZ0002 <= 0 or WZ0001 <= 0', inplace=True)
    df.sort_values(['ProductID', 'EstimateDate'], ascending=[True, True], inplace=True)

    # create two new data frame, 1 to hold the nearest presale orders, another to hold the orders need to be replenish 

    presale_orders = repl_orders = pd.DataFrame(columns=df.columns, data=None)

    # find unique productIDs
    product_list = df['ProductID'].unique()
    #print('total product=', len(product_list))
    for p in product_list:
        orders_touched = df.query('ProductID == @p and UnitReceived > 0')
        orders_untouched = df.query('ProductID == @p and UnitReceived <= 0')
        if len(orders_untouched):
            presale_orders = presale_orders.append(orders_untouched.iloc[0])
        else:
            repl_orders = repl_orders.append(orders_touched.iloc[0])
    
    no_presale = (len(presale_orders) == 0)
    if not no_presale:
        presale_orders.to_csv(
            os.path.join(os.path.dirname(filename), 'presale_orders.csv'), 
            index = False)
    no_repl = (len(repl_orders) == 0)
    if not no_repl:
        repl_orders.to_csv(
            os.path.join(os.path.dirname(filename), 'replenish_orders.csv'), 
            index = False)

    # handle master/parent BOM
    # first read the master BOM information from a dedicate file named "ProductBillOfMaterials.csv"
    fileBOM = os.path.join(os.path.dirname(filename), 'BOM.csv')

    try:
        dfBOM = pd.read_csv(fileBOM, header=0,
            names = ["ParentID",
                "ParentDesc",
                "filler",
                "filler2",
                "CompID",
                "CompDesc",
                "fill3",
                "fill4"],
            usecols = ['ParentID', 'ParentDesc', 'CompID', 'CompDesc'])
    except:
        print("BOM.csv NOT found")
        no_bom = True
    else:
        dfBOM['ParentID'].ffill(inplace=True)
        dfBOM['ParentDesc'].ffill(inplace=True)

        dfBOM.query('CompID in @product_list', inplace=True)
        no_bom = (len(dfBOM) == 0)
        if not no_bom:
            dfBOM.to_csv(
                os.path.join(os.path.dirname(filename), 'presaleBOM.csv'), 
                index = False)


        print('len of presale BOM =', len(dfBOM))
    
    # produce the download file according the status of 3 no_ variables
    if no_repl + no_presale + no_bom == 2 : there is only 1 file produced
        return 'presale_orders.csv' if not no_presale 
            else 'replenish_orders.csv' if not no_repl else 'presaleBOM.csv'
    # otherwise create a zip file
    from zipfile import ZipFile

def cbd(filename, fileCMB, CONTAINER_SIZE):
    log = open(os.path.join(os.path.dirname(filename), 'wazo.log'), 'w')
    sys.stdout = log
    orig_stdout = sys.stdout

    dfOrder = pd.read_excel(filename, header=0, names = ["SKU", "Quantity"])
    #fileCMB = os.path.join(os.path.dirname(filename), 'CustomUnitCbm.xlsx')


    try:
        dfCMB = pd.read_excel(fileCMB, header=None, skiprows=1, usecols=[1,2,3,4], 
            names = ["SKU", "Desc", "CMB", "StdPacking"])
        print(dfCMB.head(5))
    except:
        print(fileCMB+" NOT found")
        return

# ask for a container size
    dfOrder = dfOrder.join(dfCMB.set_index('SKU'), on = 'SKU', how='inner')

# handle component SKU

    def combine_component(dfOrder):
        # remove all compontent SKU rows from data frame
        dfWhole = dfOrder[dfOrder['SKU'].apply(lambda x: True if '-' not in x else False)]
        # create a dataframe of component SKUs
        dfComponent = dfOrder[dfOrder['SKU'].apply(lambda x: True if '-' in x else False)]
        if len(dfComponent) == 0: 
            return dfWhole, dfComponent

        # slit component SKU to master SKU and subset of child SKU
        dfSKU = dfComponent['SKU'].str.split('-', n=1, expand=True)
        dfSKU.columns = ['MasterSKU', 'SubSKU']
        dfComponent = pd.concat([dfSKU, dfComponent], axis=1)

        dfSKU = dfComponent.groupby(['MasterSKU']).agg({
            'StdPacking':'max', 'CMB':'sum', 'Quantity': 'max'}).reset_index()
        dfSKU.rename(columns = {'MasterSKU':'SKU'}, inplace = True)
        return dfWhole.append(dfSKU), dfComponent

    dfOrder, dfComponent = combine_component(dfOrder)
    dfOrder['NumOfUnit'] = (dfOrder['Quantity'] + dfOrder['StdPacking'] - 1) // dfOrder['StdPacking']
    print(dfOrder.head())

    order_list = dfOrder.to_dict(orient='records')

    class Container:
        def __init__(self, cmb = 65):
            self.room = self.cmb = cmb
            self.boxs = []
        
        def load(self, order):
            self.boxs += [order]
            self.room -= order['CMB'] * order['NumOfUnit']
    
    def load_container(order_list, container_size):
        container = Container(container_size)
        container_list = []

        #print('len of order', len(order_list))
        while order_list:
            order_mask = list(map(lambda x: container.room >= x['CMB'] * x['NumOfUnit'], order_list))
            try:    
                i = order_mask.index(True)    # find the first load can be loaded into the container
                order = order_list.pop(i)
                container.load(order)
            except: # no order can be loaded wholly into the rest room of current container
                # find the order that its units partial loaded, the left room of the container is the smallest
                order_mask = list(map(lambda x: container.room >= x['CMB'], order_list))
                if sum(order_mask) == 0:
                    # there is no room for ny order even for partial loading
                    # open a new container
                    container_list += [container]
                    print('created a n#ew container...\n', 'the left room is', container.room)
                    container = Container(container_size)
                    continue        # load order to the newly opened container

                # at least there is an order can be partial loaded to the container
                # fetch the order that will waste the smallest room of current container
                left_room = list(map(lambda x: container.room % x['CMB'], order_list))
                left_room = [x*y for x, y in zip(order_mask, left_room)]
                i = left_room.index(min(i for i in left_room if i > 0))
                x = order_list.pop(i)
                load_unit = container.room // x['CMB']
                x['NumOfUnit'] -= load_unit
                new_order = x.copy()
                new_order['NumOfUnit'] = load_unit
                order_list.insert(0, x)
                order_list.insert(0, new_order)
                #print('split an order, the parial load unit is', x, new_order)
        container_list += [container]
        print('container_list', len(container_list))
        return container_list

    containers, i = load_container(order_list, CONTAINER_SIZE), 1
    dfCon = pd.DataFrame(columns = ['ContainerNum', 'SKU', 'Desc', 'CMB', 'NumOfUnit', 'SumOfCMB', 'StdPacking'])
    for c in containers:
        df = pd.DataFrame(c.boxs)
        print('boxs=', len(c.boxs), len(df))
        print(df)
        df['ContainerNum'] = i
        df['Quantity'] = df['NumOfUnit'] * df['StdPacking']
        df['SumOfCMB'] = df['NumOfUnit'] * df['CMB']
        print(df['SumOfCMB'].sum(), 'items=', len(df))

        dfCon = dfCon.append(df)
    #    print(dfCon.tail())
        i += 1  # next container #

    # final step is to replace those master SKU with the component SKU
    def extract_component(dfContainer, dfComponent):

        dfMaster = dfContainer[dfContainer.SKU.isin(dfComponent.MasterSKU)]
        dfContainer = dfContainer[~dfContainer.SKU.isin(dfComponent.MasterSKU)]

        dfMaster = dfMaster.rename(columns = {'SKU': 'MasterSKU'})
        dfMaster = dfMaster.drop(columns = ['Desc', 'Quantity', 'CMB'])
        #print(dfMaster)
        dfComponent = dfComponent.drop(columns = ['StdPacking', 'SubSKU' ])
        #print(dfComponent)
        dfMaster = dfMaster.join(dfComponent.set_index('MasterSKU'), on = 'MasterSKU')
        dfMaster = dfMaster.drop(columns=['MasterSKU'])
        dfMaster['SumOfCMB'] = dfMaster['CMB'] * dfMaster['NumOfUnit']
        dfMaster['Quantity'] = dfMaster['NumOfUnit'] * dfMaster['StdPacking']
        print(dfMaster)
        #print(dfMaster.dtypes)
        return dfContainer.append(dfMaster).sort_values('ContainerNum')


    if len(dfComponent):
        dfCon = extract_component(dfCon, dfComponent)
    print(dfCon.tail())
    dfCon.to_csv(os.path.join(os.path.dirname(filename), 'container_building_block.csv'), index = False)

    log.close()
    sys.stdout = orig_stdout
    return 'container_building_block.csv'

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = ['xlsx']

app = Flask(__name__, template_folder='templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = 'the random string'

def allowed_file(filename):
    global ALLOWED_EXTENSIONS
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/container', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        for f in ['order_file', 'cmb_file']:
            if f not in request.files:
                app.logger.error('No '+ f +' part')
                return redirect(request.url)

            file = request.files[f]
            if file.filename == '':
                app.logger.warning('No selected file')
                return redirect(request.url)

            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                app.logger.info('uploading '+filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            if f == 'order_file':
                order_file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            else:
                cmb_file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        else:
            CONTAINER_SIZE = int(request.form['container'])
            app.logger.info(f'container_size={CONTAINER_SIZE}')
            filename = cbd(order_file, cmb_file, CONTAINER_SIZE)
            return redirect(url_for('downloaded_file', filename=filename))
    return render_template('upload_replenish_order.html')

@app.route('/uploads/<filename>')
def downloaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':

    app.run(debug=True, host='192.168.0.10')
