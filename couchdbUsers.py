import couchdb
import pandas as pd
import hashlib
import json
from config import host

def couchdb_to_pandas(db):
  rows =db.view('_all_docs', include_docs=True)
  data =[row['doc'] for row in rows]
  return pd.DataFrame(data)

if __name__ == "__main__":

  xlsx_file = 'CP Program Leaders.xlsx'
  df= pd.read_excel(xlsx_file)
  df.Last.replace(',','', regex=True, inplace=True)
  df['name'] = df.Last + ', '+df.First
  print(df.columns)
  print(df.head())
  print()


  couch = couchdb.Server(host)
  db_grp = couch['astro_groups']
  db_usr = couch['astro_users']

  df_grp = couchdb_to_pandas(db_grp)
  df_user = couchdb_to_pandas(db_usr)
  print(df_grp.head())

  print()
  print(df_grp.columns)
  print()
  print(df_grp.groupname.unique())
  print(df_user.columns)

  print()

  pnav_password = 'Corteva1!'
  
  for i, row in df.iterrows():
    if row['Corteva email'] not in df_user.email.values:
      newUser = {
        'username':row['Phibred ID'],
        'name':row['name'],
        'password': hashlib.md5(pnav_password.encode()).hexdigest(),
        'organisation': 'Corteva',
        'phone1': ',',
        'email': row['Corteva email'],
        'admin': {
          'force_password_change': True
        }
      }
      print(i,':')
      print(json.dumps(newUser))
      out = db_usr.save(newUser)
      print(out[0], out[1])
      print('group:  ', row['group'])

      if row['group'] in df_grp['groupname'].values:
        id = df_grp[df_grp.groupname == row['group']]['_id'].item()
        print(id)
        doc = db_grp.get(id)
        print(doc['users'])
        if out[0] not in doc['users']:
          print('Adding:  ',out[0])
          doc['users'].append(out[0])
          doc = db_grp.save(doc)
          print('Saved:  ', doc)

      print()
     pi