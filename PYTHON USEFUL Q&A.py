1.) PYTHON TO READ A REDSHIFT TABLE IN SQL AND TO USE JOINS :

import openpyxl
import pandas_redshift as pr   
import psycopg2
import pandas as pd

pr.connect_to_redshift(dbname = 'gsfindb',
                            host = 'gsf-in-analytics-1.cckzhcolnggx.eu-west-1.redshift.amazonaws.com',
                            port = 5439,
                            user = 'gsf_superuser',
                            password = 'GSFgsf2020')

connection_caspian=psycopg2.connect(dbname = 'gsfindb', host='gsf-in-analytics-1.cckzhcolnggx.eu-west-1.redshift.amazonaws.com', 
                                        port= '5439', 
                                        user= 'gsf_superuser', 
                                        password= 'GSFgsf2020')
										
query1 = pd.read_sql(""" 
    Select * from gsfindb.gsfin_fcops.uff_sop  where date between current_date -7 and current_date
    """,connection_caspian)
query2 = pd.read_sql(""" 
    Select * from gsfindb.gsfin_fcops.uff_actual_order  where date between current_date -7 and current_date
    """,connection_caspian)	
df_uff = query1
df_3p = query2
df_inner_join = pd.merge(left =df_uff, right=df_3p,how='inner',left_on='fc',right_on='fc')
df_inner_join = pd.merge(left =df_uff, right=df_3p,how='inner',left_on=['fc','date'],right_on=['fc','date'])  /* for multiple column join */
df_final = df_inner_join

2.)WINDOWS RANK FUNCTION IN PYTHON PANDAS TO FIND 3rd HIGHEST ORDER COUNT :

import pandas_redshift as pr   
import psycopg2
import pandas as pd

pr.connect_to_redshift(dbname = 'gsfindb',
                            host = 'gsf-in-analytics-1.cckzhcolnggx.eu-west-1.redshift.amazonaws.com',
                            port = 5439,
                            user = 'gsf_superuser',
                            password = 'GSFgsf2020')

connection_caspian=psycopg2.connect(dbname = 'gsfindb', host='gsf-in-analytics-1.cckzhcolnggx.eu-west-1.redshift.amazonaws.com', 
                                        port= '5439', 
                                        user= 'gsf_superuser', 
                                        password= 'GSFgsf2020')
										
query1 = pd.read_sql(""" 
    Select * from gsfindb.gsfin_fcops.uff_sop  where date between current_date -7 and current_date
    """,connection_caspian)
df_uff = query1
df_uff['rnk']= df_uff.sort_values(['snop_orders'],ascending=False).groupby(['fc','date','dslot']).cumcount()+1   /*adding new column rank */
df_result = df_uff.query('rnk==3')  OR df_result = df_uff[df_uff['rnk']==3]
df_result

3.) FIND & DELETE DUPLICATE ROWS IN PANDAS USING PYTHON:

import pandas as pd
import datetime
import datetime

df_1 =pd.read_csv(r"\\ant\Dept-as\BLR3\Groupdata\PN-IN-ANALYTICS\Inputs\Spoo_ip_3p\pio.csv")

df_1

df1[df1['City'].duplicated()]      /* gives duplicate rows based on columns */

df_1.drop_duplicates(subset=['emp_id'],inplace=True)    /*remove duplicates and give new one */

df_1

4.)AGGREGATION FUNCTION IN PANDAS PYTHON:

import pandas as pd
import datetime
import datetime

data_path = "//ant//dept-as//BLR3//Groupdata//PN-IN-ANALYTICS//Inputs//Spoo_ip_3p//pio.xlsx"
print(data_path)

df = pd.read_excel(data_path,sheet_name="plo", engine = 'openpyxl')

df

df.groupby('Category').Profit.max()    

df.groupby(['Category','City']).Profit.agg(['mean','max','min','count'])     /* multiple aggregation */

df.groupby(['Category']).agg({'Profit':max,'Sales ':min})      /* mulitple column aggregation */

df.groupby(['Category']).agg({'Profit':'mean','Sales ':min}) 

df_1 = df.groupby(['Category']).agg({'Profit':'mean','Sales ':min}).plot(kind='bar')

5.)CREATE NEW COLUMN USING IF ELSE CONDITION @CASE STAT IN SQL

import pandas as pd
import datetime
import datetime

data_path = "//ant//dept-as//BLR3//Groupdata//PN-IN-ANALYTICS//Inputs//Spoo_ip_3p//pio.xlsx"
print(data_path)

df = pd.read_excel(data_path,sheet_name="plo", engine = 'openpyxl')

df

df[df['City']=='BLR']

df.loc[df['City'].isin(['BLR','Mysore']),'State']='Karnataka'       /* if else for char */
df.loc[df['City']=='CHN','State']='TamilNadu'
df

df.loc[df['Profit']<=250,'Profit_Category']='Low_Profit'
df.loc[(df['Profit'] >250) & (df['Profit']<=500),'Profit_Category']='Medium_Profit'                /* if else for int */
df.loc[df['Profit']>500,'Profit_Category']='High_Profit'
df

6.) CREATING DATAFRAME AND USAGE OF PIVOT AND PIVOT TABLE :

import pandas as pd
import datetime
import datetime

cric = pd.DataFrame({'runs':[1,2,3,5,4,6,8,9], 'overs':[1,2,3,4,1,2,3,4],'teams':['eng','eng','eng','eng','aus','aus','aus','aus']})

cric1 = pd.DataFrame({'runs':[1,2,3,5,4,6,8,9], 'overs':[1,2,3,4,1,2,3,4],'teams':['eng','eng','eng','eng','aus','aus','aus','aus'],
                    'innings':[1,2,1,2,1,2,1,2]})

cric.pivot(columns='teams',values='runs',index='overs')

cric1.pivot_table(columns='teams',values='runs',index='innings',aggfunc='sum')   /*pivot table supports agg but pivot doesn't */

7.)CONCAT IN SERIES & DATAFRAME:

import pandas as pd

df1 = pd.DataFrame({'A':['A0','A1','A2','A3'],
                     'B':['B0','B1','B2','B3']})
					 
df2 = pd.DataFrame({'C':['C0','C1','C2','C3'],
                     'D':['D0','D1','D2','D3']})	
df5 = pd.Series([1,2,3])

df6 = pd.Series(['A','B','C'])					 

df3 = (pd.concat([df1,df2],axis=1))    /* to ignore duplicate and NULL */

df3 = (pd.concat([df1,df2],axis=0))  

df7 = pd.concat([df5,df6],axis=1)

8.) NESTED LIST WITH SORT OPERATIONS & AGGREGATION:

import pandas as pd

cricket = [['7-Nov','AFG',4,1,1], 
          ['7-Nov','ENG',3,0,2],
          ['7-Nov','IND',2,2,0],
          ['7-Nov','AUS',1,1,0]]
ddf = pd.DataFrame(cricket, columns=['Date','Teams','No_of_matches','Win','Lost'])

ddf.sort_values(['No_of_matches'],ascending=True)                     /* Asc order */
ddf.sort_values(['No_of_matches'],ascending=False)

ddf.groupby('Date').agg({'No_of_matches':sum, 'Win':max})

ddf.dtypes                    /* to find datatype */

9.) FIND CURRENT TIME & DATE WITH NEXT LEAP YEAR :

import pandas as pd
from datetime import datetime

today = datetime.today()
df = today.strftime('%D-%M-%Y %H : %M : %S')
print('Date & Time is :', df)

year = int(input('Year:'))

initial = year

while True:

year += 1
    if year % 4 == 0 and (year % 100 != 0 and year % 400 != 0):
        break

    elif year % 100 == 0 and year % 400 == 0:
        break
print(f"The next leap year after {initial} is {year}")

10.) SWAP TWO NUMBERS :

x= 5
y = 6 

temp = x
x = y
y = temp
print('value of x after swapping',x)
print('value of y after swapping',y)

11.)Change datatype of column ,remname & drop a column in dataframe :

import pandas as pd

cricket = [['7-Nov','AFG',4,1,1], 
          ['7-Nov','ENG',3,0,2],
          ['7-Nov','IND',2,2,0],
          ['7-Nov','AUS',1,1,0]]
ddf = pd.DataFrame(cricket, columns=['Date','Teams','No_of_matches','Win','Lost'])
ddf.dtypes
ddf.astype({'Win':float},copy = False)
ddf.rename(columns={'Win':'Win_Streak','Lost':'Lost_Streak'},inplace=True)
ddf.drop('Lost_Streak',axis=1,inplace=True)

12.)CONVERT A COLUMN OF LIST INTO ROWS :

dff = pd.DataFrame({'Name' : ['Hareez', 'Saya', 'Kumar'], 'Subject' :[['Maths','Science'],['History','Chemistry'],['Commerce']]})
dff
dff.explode('Subject')

13.)LAMBDA FUNCTION EXAMPLES :

square = lambda x : x**2 
square(7)

add = lambda a,b,c : a+b+c
add(10,10,100)

str = 'Python'
upper = lambda x : x.upper()
upper(str)

14.)IS , IN and NOT IN operator :

color = ['green','red','black']
print('green' not in color)    -- o/p False
print('green' in color)        -- o/p True

for colors in color:
    print(colors)              --o/p green,red,black

x = ['apple','orange','kiwi']
y =  ['apple','orange','kiwi']
print(x is y)                   --o/p False

15.) INDEX AND MULTI INDEX IN PANDAS :

import pandas as pd
import numpy as num

data = [[99,98],
        [88,87],
        [98,99],
        [87,78]]
pd.DataFrame(data)
ind = [['Bob','Bob','Lily','Lily',],
       [2019,2019,2020,2020]
      ]
df= pd.DataFrame(data,index=ind , columns= ['Math','Science'])          
df.index.names=['Name','Year']
df

              --------- Multi---- 

scores = [
    [88,45,78,99,77,22],
    [88,45,78,99,55,22],
    [88,45,78,99,44,22],
    [88,45,22,99,77,22],
    [88,56,78,99,77,22],
    [88,45,78,78,77,22]]
	
pd.DataFrame(scores)
ind = pd.MultiIndex.from_product([['school1','school2','school3'],[2019,2020]])
ind
cols = pd.MultiIndex.from_product([['7th Grade','8th Grade'],['maths','reading','writing']])
cols
dff = pd.DataFrame(scores,index=ind,columns=cols)	
dff.index.names=['school','year']
dff

16.) loc (label location) vs iloc (integer location) with slicing and loc as WHERE Statement:

Def : loc slicing works on inclusive value only , but iloc works on inclusive & exclusive value like normal slicing

import pandas as pd
import datetime
import datetime
df = pd.read_csv(r"\\ant\Dept-as\BLR3\Groupdata\PN-IN-ANALYTICS\Inputs\Spoo_ip_3p\pio.csv")
df.head(3)
df.loc[0:2,:]                   /*0,1,2th row and all columns */

df.loc[0:4,:]
df.loc[:,['City','Profit']]
df.loc[:,'Order_id':'Profit']    /*all rows and all columns */

df.loc[df.City=='BLR']           /* display only BLR city */


df.iloc[0:2,:]                    /*0,1th row and all columns */         

17.)FIND GIVEN NUM IS ODD OR EVEN USING NORMAL METHOD & USING FUNCTION :

num = int(input("Enter given number:"))
if(num%2)==0 :
    print('Even Number')
else :
    print('Odd Number')
	
----------------- Using Function -------------

num = int(input("Enter given number:"))
def even_add(num):
    if(num%2)==0 :
        print(num,'Is even number')
    else :
        print(num,'Is odd number')
even_add(num)

18.) Find the largest number among three numbers using inbuilt functions & without inbuilt functions:

a = int(input("Enter given number:"))
b = int(input("Enter given number:"))
c = int(input("Enter given number:"))

large = max(a,b,c)
print('The largest number is',large)

--------------- Using Function ---------------

a = int(input("Enter given number:"))
b = int(input("Enter given number:"))
c = int(input("Enter given number:"))

def findlargest(a,b,c):
    if a >= b and a >= c :
        print(a, "is large")
    elif b >=a and b >= c :
        print(b, "is large")
    else :
        print(c,"is large")
findlargest(a,b,c)

19.) Find given num is prime or not

num = int(input("Enter the number:"))

for i in range(2,num):
    if(num%i)==0:
        print("Not Prime")
        break
else:
    print("Prime")
	
------------ Using Function --------------------

import math
n = 11

def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True
is_prime(n)
	
20.)Reverse a string or number and find Palindrome :

txt = 'HelloWorld'
new_txt = txt[::-1]
if txt == new_txt:
    print("Palindrome")
else:
    print("Not Palindrome")
	
-------------- Using Function -----------------------

val = str(input("Enter the num or text"))
def reverse(val):
    txt = val[::-1]
    print('Reversed val is:',txt)
    if txt == val:
        print("Palindrome")
    else:
        print("Not palindrome")
reverse(val)

21.) FIND cube of value using normal function and lambda function

def cube(y):
    return y*y*y
cubes = lambda y : y*y*y
print("The cube of normal function is:",cube(3))
print("The cube of normal function is:",cubes(4))

22.)List Append , Delete , sort , reverse and slicing :

l = ['apple','banana','cherry']
l.append("kiwi")
print(l)

l.insert(2,'honey')                     /* insert value in 2nd index */
print(l)
l.extend([4,5,6])
print(l)

l.remove(4)                          /* remove number 4 in the list */
print(l)

del l[2:4]
print(l)

l.sort()
l.reverse()
print(l)

23.)Remove duplicate from single list using numpy and pandas dataframe

import numpy as np
list = [1,2,3,4,3,2,6]
a = np.unique(list)
print(a)

----------- using pandas -----------------

import pandas as pd
list = [1,2,3,4,3,2,6]
df = pd.DataFrame({'col':list})
df.drop_duplicates(inplace = True)
res = df['col'].tolist()
res

24.) Get the new list which contains only letter 'a' in the list using List & List comprehension

list = ['apple','banana','cherry']
new_list = []

for x in list:
        if 'a' in x:
            new_list.append(x)
print(new_list) 

----------- List comprehension ---------------
list = ['apple','banana','cherry']
new_list = [x for x in list if 'a' in x]
new_list

25.)CREATE A DATA FRAME AND PUSH TO EXCEL SHEET :

import pandas as pd
import numpy as num
import openpyxl 
fir = pd.DataFrame({'name':['hareez','saya','pillayar','amman'],'mark':[23,45,66,77],'country':['BLR','VNR','HEAVEN','HEAVEN']})
fir

def date_time():
    return str(date.today())
name = date_time()

writer = pd.ExcelWriter("//ant/Dept-as/BLR3/Groupdata/PN-IN-ANALYTICS/Inputs/testy" + name + ".xlsx", engine="xlsxwriter")
fir.to_excel(writer, sheet_name="Raw_data")
writer.save()

26.) FIND IF THE LIST IS HAVING NEGATIVE VALUE OR NOT AND DISPLAY NEGATIVE VALUE:

import numpy as num

user_input = input("Enter a list of numbers separated by spaces: ")
my_list = [float(num) for num in user_input.split()]

def negative_number(list):
    negative = [num for num in my_list if num < 0]
    return negative 

negative_value = negative_number(list)
if negative_value:
    print('The negative value are:',negative_value)
else:
    print('No negative value')
	
27.) FIND FIBANOCCI SERIES USING RECURSSION FUNCTION:

import numpy as num

def fibanocci_rec(n):
    if n<=1:
        return n
    else:
        return (fibanocci_rec(n-1) + fibanocci_rec(n-2))
n = 10
if n < 0:
    print('Enter crct value:')
else:
    print('Fibanocci is:')
for i in range(n):
    print(fibanocci_rec(i))
	
28.) FIND FACTORIAL USING RECURSSION FUNCTION:

import numpy as num
def factorial_rec(n):
    if n == 1:
        return n
    else:
        return n * factorial_rec(n-1)
n = 8
if n < 0:
    print('Enter valid number')
elif n == 0:
    print("factorial is 0")
else:
    print('Factorial is:',factorial_rec(n))
	
29.)MASKING EMAIL_ID USING SPILT WITH AND WITHOUT FUNCTION :

email = "hareesh@gmail.com"
username, domain = email.split('@')
mask = username[0] + '*' * (len(username)-1) + domain
mask
------------------------------- USING FUNCTION -------------------------------
def mask_email(email):
    username, domain = email.split("@")
    mask = username[0] + "*" * (len(username)-1) + domain
    return mask

email = "hhareesh@gmail.com"
masked_email = mask_email(email)
print(masked_email)

30.) REPLACE A LETTER 'D' TO 'S' IN THE ROWS :

import pandas as pd

data = {'Name':['Mayana','Dayanad','Hayana'],
       'Age': [23,45,56]}
df= pd.DataFrame(data)

df['Name'] = df['Name'].replace({'d': 's', 'D': 'S'}, regex=True)
df

30.)REMOVE DUPLICATE FROM 2 DATAFRAME AND CONVERT TO LIST:

import pandas as pd
list1 = [1,2,3,4,3,2,6]
list2 =['ku','mu','ju','hu','ju','mu','zu']
df = pd.DataFrame({'col1':list1,'col2':list2})

dffd= df.drop_duplicates(subset=['col2'])
list_from_df = dffd.values.tolist()

31.)FIND DUPLICATE IN THE GIVEN LIST :

lists = [1,2,3,4,5,3,4]
seen = set()
duplicates = []

for items in lists:
    if items not in seen:
        seen.add(items)
    else:
        duplicates.append(items)
        
print('Duplicates are',duplicates )

32.)COUNT OF OCCURANCE OF CHAR IN LIST IN 2 WAYS :

name = 'Hareesh'
txt = 'e'
ans = name.count(txt)
print('Occurance of letter e is:',ans)

-------------------------- FOR ALL LETTERS -----------------------
name = 'Hareeshwhyagain'

char_counts = {}

for txt in name:
    char_counts[txt] = char_counts.get(txt,0)+1
    
for txt, count in char_counts.items():
    print(txt,'-',count)
    
33.) FIND THE NAME WITH 2nd WORD AS 'A' IN THE LIST :

names = ['aaple','banana','cherry']
second_letter = []

for items in names:
    if len(items) >1 and items[1].lower()=='a':
        second_letter.append(items)
print('Second letter a name is:', second_letter)

-------------------- Using List Comphersion -----------------------------------

names = ['aaple','banana','cherry']

second_letter = [new_name for new_name in names if len(new_name)>1 and new_name[1].lower()=='a']

print('Second letter a name is',second_letter)

34.)TRANSPOSE AN ARRAY :

import numpy as num
a = num.array([[1,2,2],[2,2,3],[3,4,4]])
b = a.transpose()
print('The transposed array is :',b)

35.)AMSTRONG NUMBER IN PYTHON:

number = int(input("Enter a number to check for Armstrong property: "))

def is_armstrong_number(number):
    
    num_str = str(number)
    num_digits = len(num_str)
    armstrong_sum = sum(int(digit) ** num_digits for digit in num_str)
    
    return armstrong_sum == number

if is_armstrong_number(number) is True:
    print('The given num is a amstrong number')
else :
    print('The given num is not a amstrong number')
    
36.)USE MATPLOTLIB AND DRAW LINE , BAR AND SCATTER CHART:

import matplotlib.pyplot as plt

x = [1, 2, 3, 4, 5]
y = [2, 4, 6, 8, 10]

plt.plot(x, y, color='green', label='Line Chart')
plt.xlabel('X-axis')
plt.ylabel('Y-axis')
plt.title('Simple Line Plot')
plt.legend()
plt.show()
----------------------------- BAR CHART -------------------------------
import matplotlib.pyplot as plt 

x = [2,3,4,5]
y = ['Kumar','Harees','Saya','Pillayar']

plt.bar(x,y,color='green',label = 'Bar chart')
plt.xlabel('X axis')
plt.ylabel('Y axis')
plt.title('Simple Bar')
plt.show()
---------------------------------- SCATTER PLOT ------------------------
import matplotlib.pyplot as plt 

x = [2,3,4,5]
y = ['Kumar','Harees','Saya','Pillayar']

plt.scatter(x,y,color='green',label = 'Scatter plot')
plt.xlabel('X axis')
plt.ylabel('Y axis')
plt.title('Simple Bar')
plt.show()

37.)USE SEABORN LIB AND DRAW LINE , BAR AND SCATTER CHART AND PERFROM AGGREGATION:

import seaborn as sns
import matplotlib.pyplot as plt

df = pd.DataFrame({
    'Product_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'Flavor_Group': ['A', 'B', 'A', 'C', 'B', 'C', 'A', 'B', 'C', 'A']
})

group_performance = df.groupby('Flavor_Group').Product_id.sum()

sns.lineplot(x=XX, y=YY, data = group_performance ,color='skyblue')
plt.xlabel('Categories')
plt.ylabel('Values')
plt.title('Seaborn Line Chart')
plt.show()

------------------------ BAR CHART ---------------------------
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

df = pd.DataFrame({'Product_Name': ['Glass', 'Water', 'Los', 'Water'],
                   'Cost': [22, 33, 44, 44]})

# Group by Product_Name and sum the Cost
datas = df.groupby(['Product_Name']).Cost.sum().reset_index()

# Plotting with seaborn
sns.barplot(x='Product_Name', y='Cost', data=datas, palette='pastel')
plt.xlabel('X-axis')
plt.ylabel('Y-axis')
plt.title('Demo')
plt.show()
--------------------------- SCATTER PLOT -------------------------

import seaborn as sns
import matplotlib.pyplot as plt

df = pd.DataFrame({
    'Product_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'Flavor_Group': ['A', 'B', 'A', 'C', 'B', 'C', 'A', 'B', 'C', 'A']
})

group_performance = df.groupby('Flavor_Group').Product_id.sum().reset_index()

sns.scatterplot(x='Product_id', y='Flavor_Group', data = group_performance ,palette='viridis')
plt.xlabel('Categories')
plt.ylabel('Values')
plt.title('Seaborn Scatter Chart')
plt.show()

38.) DROPNA AND FILLNA FOR MISSING VALUE:

import pandas as pd
data = {
    'Name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
    'Age': [25, 30, None, 22, 28],
    'City': ['New York', 'Paris', 'London', None, 'Tokyo']
}
df = pd.DataFrame(data)
df_drop = df.dropna()
df_drop

df_fill = df.fillna(df.mean())
df_fill

39.)TRY AND EXCEPT HANDLING IN PYTHON :

try:
    user_input = int(input("Enter a number: "))
except ValueError:
    print("Invalid input. Please enter a valid number.")
else:
    result = user_input * 2
    print(f"The result of doubling the input is: {result}")
finally:
    print("Finally block executed.")
    
40.) STAR PATTERN IN PYTHON :

rows = 5 
for i in range(1,rows+1):
    for j in range(1,i+1):
        print('*', end=' ')
    print('')
    
41.) FIND THE STUDENT WHOSE PERCENTAGE IS MORE THAN 90% IN TOTAL MARK AND DISPLAY ONLY ROLL_NO AND PERCENTAGE COLUMN:

import pandas as pd
data = {
    'roll_no': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
    'subj': ['a', 'b', 'c', 'd', 'e', 'a', 'b', 'c', 'd', 'e'],
    'marks': [90, 90, 91, 90, 92, 64, 60, 87, 54, 52]}
df = pd.DataFrame(data)
df['total_marks'] = df.groupby(['roll_no']).marks.sum()

df['avg_percentage'] = df.groupby(['roll_no']).marks.mean()
result = df[df['avg_percentage'] >= 90]

output = result[['roll_no', 'total_marks', 'avg_percentage']].drop_duplicates()
output

42.) WRITE A PROG TO PRINT THE LIST OCCURANCE AFTER THE GIVEN INPUT VALUE :

def print_after_input(input_value, input_list):
    try:
        index_of_input = input_list.index(input_value)
        output = input_list[index_of_input + 1:]
        print(output)

    except ValueError:
        print(f"The input value '{input_value}' is not present in the list.")

input_list = ['z', 'b', 'c', 1, 2, 'x', 'y', 'a', 3]
input_value = input("Enter the input value: ")

print_after_input(input_value, input_list)

43.) PRINT THE DATA FRAME WITH NULL VALUE AND WITHOUT NULL VALUES:

import pandas as pd 
data = {'no': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
     'class': ['z', 'z', 'z', 'z', 'z', 'o', 'o', 'o', 'o', 'o'], 
    'sub': ['a', 'b', 'c', 'd', 'e', 'a', 'b', 'c', 'd', 'e'],
    'mark': [90, 90, 91, 90, 92, 64, 60, 87, 54, 52]}

new = pd.DataFrame(data)
new['total_mark'] = new.groupby(['no','class']).mark.sum().reset_index(drop=True)  #use reset_index when u are grouping multiple columns and push to one column

res = new[['sub','class','total_mark']].drop_duplicates()

res[res['total_mark'].isna()]      # to get null value #
res[res['total_mark'].notna()]      # to get not null value #
res

44.)FIND THE CUSTOMER WHO WHO PLACED UNIQUE MAXIMUM ORDERS AND MINIMUM ORDERS :

import pandas as pd

# Sample data
data = {'cust_id': [1,1,1,2,2, 3, 4, 5, 6, 7, 8, 9,9, 10],
    'order_id':['xx','yy','oo','as','ac','ee','cc','bb','rr','tt','jj','ff','ff','dd'],
    'item_id' : ['a','b','c','a','b','a','a','a','a','a','a','a','b','a'],
    'price': [91,23,55, 92,24, 93, 90, 88, 64, 60, 87, 54,81, 99]}

ddf = pd.DataFrame(data)
ddf
order = ddf.groupby(['cust_id']).order_id.nunique().reset_index().rename(columns={'order_id':'count_of_order'})     #nunique for distinct count
order.sort_values(['count_of_order'],ascending = False)

max_order_cust = order.loc[order['count_of_order'].idxmax()]                              # index of maximum value 
min_order_cust = order.loc[order['count_of_order'].idxmin()]
print(max_order_cust)
print(min_order_cust)

45.)FIND AVERAGE AND WEIGHTED AVERAGE FOR BUSINESS :

import pandas as pd 
import numpy as num 
ds = {'business': ['tennis','tennis','tennis','cricket','cricket','baseball','baseball'],
       'sales' : [2300,2344,2111,4333,4444,2345,1345]}
df = pd.DataFrame(ds)
df.groupby(['business']).sales.mean()    # Average 

count_of_sales = df.groupby(['business']).sales.count().reset_index().rename(columns = {'sales':'count_of_sales'})
sum_of_sales = df.groupby(['business']).sales.sum().reset_index().rename(columns = {'sales':'sum_of_sales'})

df_join = pd.merge(left=count_of_sales,right=sum_of_sales,how='inner',left_on='business',right_on='business')     
df_join['weighted_sum'] = df_join.count_of_sales * df_join.sum_of_sales
df_join

wght_avg = df_join.sum_of_sales.sum() / df_join.count_of_sales.sum()
wght_avg

46.)FIND INACTIVE CUSTOMER WHOSE ORDER DATE IS LESS THAN 365 DAYS FROM CURRENT DATE 

import pandas as pd
from datetime import date
# Create DataFrame
df = pd.DataFrame({'cust_id' : [123,123,123,234,234,566,566],
                   'business': ['tennis-day', 'tennis-day', 'tennis-day', 'cricket', 'cricket', 'baseball', 'baseball'],
                   'dates': ['2021-06-03', '2020-06-03', '2016-06-02', '2023-06-03', '2022-06-01', '2019-06-03', '2021-08-03']})

df['dates']= pd.to_datetime(df['dates'])
df
today_date = pd.to_datetime(date.today())                                             #covert obj to date datatype
df['last_order_date'] = df.groupby(['cust_id'])['dates'].transform('max')                #to add extra column in dataframe
df['date_diff'] = today_date - df['last_order_date']

df['date_num'] = df['date_diff'].dt.days                                                 #attribute to extract the numeric value 
df['business_name'] = df['business'].str.split('-', expand=True)[0]

ddf = df[df['date_num']>365]['cust_id'].unique()

47.)FIND DAY , WEEK , MONTH , YEAR DIFFERENCE B/W FIRST AND LAST PLACED ORDER :

import pandas as pd
from datetime import date
# Create DataFrame
df = pd.DataFrame({'cust_id' : [123,123,123,234,234,566,566],
                   'business': ['tennis-day', 'tennis-day', 'tennis-day', 'cricket', 'cricket', 'baseball', 'baseball'],
                   'dates': ['2021-06-03', '2020-06-03', '2016-06-02', '2023-06-03', '2022-06-01', '2019-06-03', '2021-08-03']})

df['dates']= pd.to_datetime(df['dates'])
df['last_order_date'] = df.groupby(['cust_id'])['dates'].transform('max')  # to add extra column in dataframe
df['first_order_date'] = df.groupby(['cust_id'])['dates'].transform('min')  # to add extra column in dataframe

df['day_diffe'] = (df['last_order_date'] - df['first_order_date']).dt.days
df['week_diffe'] = (df['last_order_date'] - df['first_order_date']).dt.days//7
df['month_diffe'] = (df['last_order_date'] - df['first_order_date']).dt.days//30
df['year_diffe'] = (df['last_order_date'] - df['first_order_date']).dt.days//365

df
res = df[['cust_id','business','last_order_date','first_order_date','day_diffe','week_diffe','month_diffe' ,'year_diffe']].drop_duplicates()


48.)CREATE NEW COLUMN USING IF ELSE CONDITION @CASE STAT IN SQL WITH NUMPY :

import pandas as pd 
import numpy as num

cr = pd.DataFrame ({'team':['a','b','c'], 'overs':[5,7,6] ,'runs':[100,123,112]})
crc = pd.DataFrame ({'team':['a','b','c'], 'name':['aus','eng','ind'] , 'month':[3,4,5]})

dffs = pd.merge(left = cr , right = crc ,how = 'inner',left_on= (['team']),right_on =(['team']))

conditions = [
    (dffs['overs'] > 0) & (dffs['overs'] <= 5),
    (dffs['overs'] > 6) & (dffs['overs'] <= 10)]

choices = ['0-5', '6-10']

dffs['over_scale'] = num.select(conditions, choices, default='10+')


49.)FIND 2nd HIGHEST VALUE IN LIST :

import pandas as pd 
list3 = [1, 100, 150, 312, 400, 10, 400, 450]

dff = pd.DataFrame({'name': list3})

dff['rank'] = dff['name'].rank(ascending=False,method='min')
dff[dff['rank']==2]

                (OR)    
                
list1 = [1, 100, 150, 312, 400, 10, 400, 450]

unique_values = set(list1)

max_value = max(unique_values)

second_highest_value = max(value for value in unique_values if value < max_value)

print(second_highest_value)

50.) RANK VS DENSE RANK :

import pandas as pd 
list3 = [1, 100, 150, 312, 400, 10, 400, 450]

dff = pd.DataFrame({'name': list3})

dff['rank'] = dff['name'].rank(ascending=False,method='min')
dff[dff['rank']==2]

                (DENSE) 

import pandas as pd
list1 = [100, 200, 300, 300, 400, 500, 600]

df = pd.DataFrame({'name': list1})
df = df.sort_values(['name'], ascending=False)

df['rank'] = range(1, len(df) + 1)
df

51.) GET THE OUTPUT FOR STATE NAME WITH KARNATAKA AND SALES GREATER THAN EQUAL TO  100:

import pandas as pd

data = {'State': ['Karnataka','TamilNadu','Karnataka','TamilNadu','Trivandram'],
        'Sales':[100,99,88,101,340]}
        
ddf = pd.DataFrame(data)
df_res = ddf[(ddf['State']=='Karnataka') & (ddf['Sales'] >= 100)]
df_res

52.) GET MIN AND MAX OF MARK :

import pandas as pd

c1 = pd.DataFrame({'Name':['AAA','BBB','CCC','DDD','EEE'],
                   'Subj':['Eng','Mat','Tam','Eng','Tam'],
                   'Mark':[67,44,55,66,77]
                  })
c2 = pd.DataFrame({'Name':['AAA','BBB','CCC','DDD','EEE'],
                   'City':['BLR','HYD','BLR','HYD','BLR'],
                   'Age':[23,23,24,25,26]
                  })
                  
df1 = pd.merge(left =c1,right =c2,how ='inner',left_on='Name',right_on='Name')

df1['Mark'].max()                

df1['Mark'].min()
