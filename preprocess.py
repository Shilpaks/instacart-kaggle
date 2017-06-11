
import pandas as pd

def row_count_distribution(df, col_name):
    df_=df[[col_name]].copy()
    df_["count"]=1
    df_=df_.groupby(col_name).sum().sort_values(by="count", ascending=False)
    return df_.reset_index()

df_prior=pd.read_csv("data/order_products__prior.csv.gz")
df_train=pd.read_csv("data/order_products__train.csv.gz")

df=pd.concat([df_prior, df_train], axis=0)
df_orders=pd.read_csv("data/orders.csv.gz")
USE_THRESHOLD=5

df_prodcount=row_count_distribution(df, "product_id")
df_prodcount.product_id[df_prodcount["count"] > USE_THRESHOLD]
df_prodcount["new_product_id"]=range(1, 1+len(df_prodcount))
    
#save list to data directory
df_prodcount.to_csv("data/processed/prodcount.csv.gz", compression="gzip")

#rename_product ids in the prior set
print("processing prior file")
df_=df_prior[["product_id"]].merge(df_prodcount[["product_id", "new_product_id"]], how="inner")
df_prior["product_id"]=df_["new_product_id"]
df_prior.to_csv("data/processed/order_products__prior.csv.gz", compression="gzip") 


print("processing train file")

df_=df_train[["product_id"]].merge(df_prodcount[["product_id", "new_product_id"]], how="inner")
df_train["product_id"]=df_["new_product_id"]
df_train.to_csv("data/processed/order_products__train.csv.gz", compression="gzip") 

print("processing user ids")

df_=pd.DataFrame(dict(user_id=sorted(set(df_orders.user_id))))
df_["new_user_id"]=range(1, len(df_)+1)
df__=df_orders[["user_id"]].merge(df_)
df_orders["user_id"]=df__["new_user_id"]
df_orders.to_csv("orders.csv.gz", compression="gzip")

