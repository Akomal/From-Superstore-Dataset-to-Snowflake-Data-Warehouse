
import pandas as pd

def splitting_files():

    dataset= pd.read_excel('./Global Data Superstore.xls')

    customers_df=pd.DataFrame(dataset,columns=['Order ID','Customer ID', 'Customer Name', 'Segment', 'City', 'State', 'Country',
           'Postal Code', 'Market', 'Region', 'Product ID'])
    #customers_df.columns= [x.lower().replace(" ","_") for x in customers_df.columns]

    orders_df=pd.DataFrame(dataset,columns=['Customer ID','Product ID','Order ID', 'Order Date', 'Ship Date', 'Ship Mode'
           ,'Sales', 'Quantity', 'Discount','Profit', 'Shipping Cost', 'Order Priority'])
    orders_df.columns = [x.lower().replace(" ", "_") for x in orders_df.columns]

    products_df=pd.DataFrame(dataset,columns=['Order ID','Customer ID','Product ID', 'Category',
           'Sub-Category', 'Product Name'])
    products_df.columns = [x.lower().replace(" ", "_") for x in products_df.columns]
    return customers_df,orders_df,products_df

def cleaning_df(products_df):

    products_df[['product_name', 'product_desc']] = products_df['product_name'].str.split(',', 1, expand=True)
    return products_df


def generate_csvfiles(customer,orders,products):



    products.to_csv('./data/products.csv', header=True, index=False)
    customer.to_csv('./data/customers.csv', header=True, index=False)
    orders.to_csv('./data/orders.csv', header=True, index=False)

if __name__ == "__main__":
    customer,orders,products=splitting_files()
    products=cleaning_df(products)

    generate_csvfiles(customer,orders,products)

