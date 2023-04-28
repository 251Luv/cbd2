import os
import pandas as pd
import sys
#import openpyxl

def cbd(filename, fileCMB, CONTAINER_SIZE):

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

    return 'container_building_block.csv'
