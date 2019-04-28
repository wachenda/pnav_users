import couchdb
import pandas as pd
import numpy as np
import hashlib
import json
from config import host

def couchdb_to_pandas(db):
  rows =db.view('_all_docs', include_docs=True)
  data =[row['doc'] for row in rows]
  return pd.DataFrame(data)

if __name__ == "__main__":

  # xlsx_file = 'CP Program Leaders.xlsx'
  # df= pd.read_excel(xlsx_file)
  # df.Last.replace(',','', regex=True, inplace=True)
  # df['name'] = df.Last + ', '+df.First
  # print(df.columns)
  # print(df.head())
  # print()


  couch = couchdb.Server(host)
  db_grp = couch['astro_groups']
  db_usr = couch['astro_users']

  df_grp = couchdb_to_pandas(db_grp)
  df_grp = df_grp[~df_grp.groupname.isna()]
  df_user = couchdb_to_pandas(db_usr)
  df_user = df_user[~df_user.username.isna()]
  print(df_grp.head())

  print()
  print(df_grp.columns)
  print()
  print(df_grp.groupname.unique())
  print(df_user.columns)

  print()
  print(df_grp[['groupname','users']])

  print()
  print(df_user[['_id','username','email','name','organisation']])

  # print()
  # print(df_grp[~df_grp.groupname.isna()])
  # print()


  user_id = '41eba833b6f1fb5e9d6b154afa32f3d2'



  # searchTerm = 'something'
  # df_user['groups'] = np.nan


  grp_lst = []
  for i, r in df_user.iterrows():
    # print(r.username)
    g = list(df_grp[[r['_id'] in x for x in df_grp.users]].groupname)
    print(i, g)
    grp_lst.append(g)
    # df_user.at[i,'groups'] = g

    # df_user.at[i, 'groups'] = list(df_grp[[r['_id'] in x for x in df_grp.users]].groupname)
    # df_user.at[i,'groups'] = list(df_grp[[r['_id'] in x for x in df_grp.users]].groupname)
    # print(r.username, list(df_grp[[r['_id'] in x for x in df_grp.users]].groupname))
    # print(r.username, r.groups)
  print()
  print(grp_lst)
  df_user['groups'] = grp_lst
  print()
  print()
  df_sav = df_user[['username','name','email','groups','organisation','admin']].copy()

  df_sav.sort_values(by=['username'],inplace=True)

  df_sav.to_excel('pnav_user_list.xlsx',index=False)
  
  # print(df_user[['username','groups']])
  # print(user_id, type(user_id))
  # for i,row in df_grp.iterrows():
  #   if user_id in row.users:
  #     print(row.groupname)

  # print()
  # print(df_user[df_user.username.isna()])
  #   for u in row.users:
  #     print(u)
    # print()
  # print(df_grp[df_gr]p['users'].isin([id])])
  # print(df_grp.users[(id in list(df_grp.users))])
  # print(df_grp[id in df_grp['users']])
  # pnav_password = 'Corteva1!'
  
  # for i, row in df.iterrows():
  #   if row['Corteva email'] not in df_user.email.values:
  #     newUser = {
  #       'username':row['Phibred ID'],
  #       'name':row['name'],
  #       'password': hashlib.md5(pnav_password.encode()).hexdigest(),
  #       'organisation': 'Corteva',
  #       'phone1': ',',
  #       'email': row['Corteva email'],
  #       'admin': {
  #         'force_password_change': True
  #       }
  #     }
  #     print(i,':')
  #     print(json.dumps(newUser))
  #     out = db_usr.save(newUser)
  #     print(out[0], out[1])
  #     print('group:  ', row['group'])

  #     if row['group'] in df_grp['groupname'].values:
  #       id = df_grp[df_grp.groupname == row['group']]['_id'].item()
  #       print(id)
  #       doc = db_grp.get(id)
  #       print(doc['users'])
  #       if out[0] not in doc['users']:
  #         print('Adding:  ',out[0])
  #         doc['users'].append(out[0])
  #         doc = db_grp.save(doc)
  #         print('Saved:  ', doc)

  #     print()
  