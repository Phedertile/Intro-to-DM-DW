#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
print(pd.__version__)


# In[184]:


# source file

# index_col = 0 is letting the table start from the first column, no additional index
orderSource1 = pd.read_excel('ETLDataSource1',sheet_name='orderSource1')
productSource1 = pd.read_excel('ETLDataSource1', sheet_name='productSource1')
stateLookupSource1 = pd.read_excel('ETLDataSource1', sheet_name='StateLookup')
orderSource2 = pd.read_excel('ETLDataSource2', sheet_name='orderSource2')
productSource2 = pd.read_excel('ETLDataSource2', sheet_name='productSource2')

# id setup
orderSource1 = orderSource1.set_index("OrderID")
orderSource2 = orderSource2.set_index("OrderID")
productSource1 = productSource1.set_index("OrderID")
productSource2 = productSource2.set_index("OrderID")

# Reset index to switch the order of table
stateLookupSource1 = stateLookupSource1.reset_index()

# just switch name
# stateLookupSource1.columns = new_cols
# stateLookupSource1 = stateLookupSource1[new_cols]

# Change both column and value
stateLookupSource1 = stateLookupSource1.reindex(columns = ["Abbreviation", "State"])
stateLookupSource1 = stateLookupSource1.set_index("Abbreviation")

# productSource1


# In[185]:


# Datasource1

joined_source1 = orderSource1.join(productSource1, how = "inner", on = 'OrderID')

# nested_dict with {"state" : {"abb" : "fullname"}}
dict = stateLookupSource1.to_dict()["State"]
joined_source1_replaced = joined_source1.replace({"CustomerState" : dict})

# Splitting CustomerName into CustomerFirstName & CustomerLastName seperating by " "
joined_source1_replaced[['CustomerFirstName', 'CustomerLastName']] = joined_source1_replaced['CustomerName'].str.split(' ', expand=True)
joined_source1_replaced_splited = joined_source1_replaced.reindex(sorted(joined_source1_replaced.columns), axis=1)
                                                                  
# joined_source1_replaced_splited


# In[186]:


# Datasource2

joined_source2 = orderSource2.join(productSource2, how = "inner", on = 'OrderID')
joined_source2 = joined_source2.reset_index()

# Edit OrderID format from A10xxx -> 10xxx
joined_source2['OrderID'] = joined_source2['OrderID'].str.replace('A','')

## parse number ??

joined_source2 = joined_source2.set_index("OrderID")

# Mapping CustomerStatus
dict2 = {1 : "Silver", 2 : "Gold", 3 : "Platinum"}
joined_source2_replaced = joined_source2.replace({"CustomerStatus" : dict2})

# Dropping TotalDiscount then recreate it
joined_source2_replaced_dropped = joined_source2_replaced.drop("TotalDiscount", axis = 1)
joined_source2_replaced_dropped["TotalDiscount"] = joined_source2_replaced_dropped["FullPrice"] - joined_source2_replaced_dropped["ExtendedPrice"]

# Ascending sorting
joined_source2_replaced_dropped_ordered = joined_source2_replaced_dropped.reindex(sorted(joined_source2_replaced_dropped.columns), axis=1)

# joined_source2_replaced_dropped_ordered


# In[187]:


# Merge Data

Merged_data = joined_source1_replaced_splited.append(joined_source2_replaced_dropped_ordered)
Merged_data["CustomerName"] = Merged_data[["CustomerFirstName", "CustomerLastName"]].apply("_".join, axis=1)
Merged_data_dropped = Merged_data.drop(["CustomerFirstName", "CustomerLastName"], axis=1)
Merged_data_dropped.to_excel("fromPandas_output.xlsx")

# Merged_data_dropped


# In[ ]:




