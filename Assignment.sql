
---------Meta Data ------

---Customer Table 

create table customer_mast(cus_id_n number ,cus_name_v varchar2(100));

alter table customer_mast add cus_age_n number;

alter table customer_mast add constraint cus_id_pk primary key ( cus_id_n);


create table sales_trans( sale_id_n number ,sale_cus_id_n number );

alter table sales_trans add constraint  sale_id_pk primary key  (sale_id_n);

ALTER TABLE sales_trans ADD CONSTRAINT sale_cus_id_fk
  FOREIGN KEY (sale_cus_id_n )
  REFERENCES customer_mast(cus_id_n);
 
 ---Order Table 
 
 create table order_trans( ord_id_n number , ord_sales_id_n number ,ord_item_id_n number ,ord_qty_n number);
 
 alter table order_trans add constraint  ord_id_pk primary key  (ord_id_n);
 
 ALTER TABLE order_trans ADD CONSTRAINT ord_sales_id_fk
 FOREIGN KEY (ord_sales_id_n )
 REFERENCES sales_trans(sale_id_n);
 
 ALTER TABLE order_trans ADD CONSTRAINT ord_item_id_fk
 FOREIGN KEY (ord_item_id_n )
 REFERENCES item_mast(item_id_n);
 
 ---Item Table 
 
 create table item_mast( item_id_n number ,item_name varchar2(100));
 
 alter table item_mast add constraint  item_id_pk primary key  (item_id_n);
 
 --------SQL Query-------
 
 Each item brought per customer aged group of 28 to35
 
  SELECT sale_cus_id ,ord_item_id_n, count(*) number_item FROM order_trans ord ,
 (SELECT sale_id_n ,sale_cus_id FROM sales_trans where sale_cus_id_n = cus_id_n AND AGE BETWEEN 28 TO 35 ) sale  WHERE ord.ord_sales_id_n= sale.sale_id_n
  group by sale_cus_id , ord_item_id_n HAVING COUNT(*) > 1;
 
pPython script  
  
import csv

import Cx_Oracle

import csv 

con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')

cur = con.cursor()

cur.execute(sql_query)


# Write to CSV
filename = 'output.csv'
with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file,  delimiter=';')
    column_names = [i[0] for i in cur.description]
    writer.writerow(column_names)
    for row in cur:
        writer.writerow(row)

# Cleanup
cur.close()
conn.close()

-----
Alternative solution in PD data frame  -------

import import pandas as pd
---empty arrary 

sale_cus_id_a=[]
ord_item_id_n_a=[]
number_item_a=[]

query = "SSELECT sale_cus_id ,ord_item_id_n, count(*) number_item FROM order_trans ord ,
 (SELECT sale_id_n ,sale_cus_id FROM sales_trans where sale_cus_id_n = cus_id_n AND AGE BETWEEN 28 TO 35 ) sale  WHERE ord.ord_sales_id_n= sale.sale_id_n
  group by sale_cus_id , ord_item_id_n HAVING COUNT(*) > 1"
with connection.cursor() as cursor:
     cursor.execute(query)
     results = cursor.fetchall()
	 
for rows in results:
    	 
	sale_cus_id_a.append(rows[0])
     	
	ord_item_id_n_a.append(rows[1])
	
	number_item_a.append(rows[2])
	
#Create a panda data frame 

df=pd.DataFrame({"sale_cus_id_n":sale_cus_id_a, "ord_item_id_n":ord_item_id_n_a,"number_item":number_item_a},index= sale_cus_id_a)

create Csv file from panda data frame ...

df.to_csv('output.csv', sep=';', index=False)	
	
	
		
		
		


  
  
  
  
  
  
  
 
 
 
 
 
 
 
 
 
 
 