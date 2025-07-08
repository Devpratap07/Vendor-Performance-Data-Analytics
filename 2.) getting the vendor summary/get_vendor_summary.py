import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(  #basicCof define the log structure
    filename = "logs/get_vendor_summary.log", # same as python file
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",  # time -> level -> msg
    filemode ="a" # a -> append
)

def create_vendor_summary(conn):
    vendor_sales_summary = pd.read_sql_query(""" with FreightSummary as(
        select
        VendorNumber,
        SUM(freight) as FreightCost
        from vendor_invoice
        group by VendorNumber
),

PurchaseSummary as(
    select
    p.VendorNumber, 
    p.VendorName,
    p.Brand, 
    p.Description,
    p.PurchasePrice,
    pp.Volume,
    pp.Price as ActualPrice,
    SUM(p.Quantity) as TotalPurchaseQuantity,
    SUM(p.Dollars) as TotalPurchaseDollars 
    from purchases p 
    join purchase_prices pp 
    on p.Brand = pp.Brand
    where p.PurchasePrice > 0
    group by p.VendorNumber , p.VendorName , p.Brand , p.Description , p.PurchasePrice , pp.Price , pp.Volume
),

SalesSummary as(
    select
    VendorNo,
    Brand,
    SUM(SalesDollars) as TotalSalesDollars,
    SUM(SalesPrice) as TotalSalesPrice,
    SUM(SalesQuantity) as TotalSalesQuantity,
    SUM(ExciseTax) as TotalExciseTax
    from sales
    group by VendorNo,Brand
)
select
    ps.VendorNumber, 
    ps.VendorName,
    ps.Brand, 
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalSalesQuantity,
    ss.TotalExciseTax,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    fs.FreightCost as TotalFreightCost
    from PurchaseSummary ps
    left join SalesSummary ss
    on ps.VendorNumber = ss.VendorNo
    and ps.Brand = ss.Brand
    left join FreightSummary fs
    on ps.VendorNumber = fs.VendorNumber
    order by TotalPurchaseDollars desc""",conn)
    
    return vendor_sales_summary

def clean_data(df):
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0,inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df ['Description'] = df['Description'].str.strip()
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars']) * 100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalestoPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    return df

if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')
    
    logging.info('Creating vendor Summary table...')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning data..')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data...')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')

    
   