from extract_functions import *


data_dict ={}
for i, file in enumerate(zip_files(source_dir)):
    print(i, end='\r')
    file_dir = f'{source_dir}\\{file}'
    with ZipFile(file=file_dir) as zip_file:
        bs = zip_file.read(zip_file.namelist()[0]).decode('utf8')
        soup = BeautifulSoup(bs, "html.parser")
        new_dict = get_all_data_from_site(soup,file[:-4])
        data_dict = new_dict if not data_dict else merge_dicts(data_dict,new_dict)
        if i == 99999:
            break


df = pd.DataFrame(data_dict,dtype=str)
df = df.apply(lambda x: x.astype(str).str.lower())
#rename cols
df.columns = columns1

#remove common words from cols
remove_list = ["m²","€",r"n.a","none","(",")", ". og","minuten zu fuß entfernt","minute zu fuß entfernt"]
for col in list(df.columns):
    for item in remove_list:
        df[col] = df[col].str.replace(item,"", regex=False)
        
#remove individual words from col
df["personen"] = df["personen"].str.replace("er","")

df["straße"] = df.adresse.str.extract(pat="(.*)\d\d\d\d\d")
df["straße"] = df.straße.str.replace("str\.","straße")
df["straße"] = df.straße.str.replace("str ","straße")
df["straße"] = df.straße.str.replace("strasse","straße")
df["straße"] = df.straße.str.replace("[^\w\d]","")
df["straße"] = df.straße.str.replace("[0-9]+","")
df["straße"] = df.straße.str.replace("ß","ss")
df.loc[~df["straße"].isin(list(pd.DataFrame(df.straße.value_counts()).query("straße > 10").index)), "straße"] = "no_info_or_rare"

# Vermutlich ist möbliert, teilmöbliert = teilmöbliert
df["bed"]      = df.bed.str.replace("möbliert, teilmöbliert","teilmöbliert")


df["geschlecht"] = df["geschlecht"] + df["geschlecht2"]
df["geschlecht"] = df["geschlecht"].str.replace("egalegal","egal")
# drop unused cols
#,"Adresse"
df = df.drop(columns=["geschlecht2"])

df["folder-closed"] = df["folder-closed"].str.replace("haustiere erlaubt","haustiere")
df["bath-bathtub"] = df["bath-bathtub"].str.replace("eigenes bad","eigenes_bad")
df["bath-bathtub"] = df["bath-bathtub"].str.replace("gäste wc","gäste_wc")


one_hot_cols = ["folder-closed",'fabric','bath-bathtub',"display"]
class_list = [['', 'aufzug','balkon','fahrradkeller', 'garten', 'gartenmitbenutzung','haustiere','keller','spülmaschine','terrasse','waschmaschine'],
              ['', 'dielen','fliesen','fußbodenheizung','laminat','parkett','pvc','teppich'],
              ['', 'badewanne','badmitbenutzung','dusche','eigenes_bad','gäste_wc'],
              ['', 'kabel','satellit']]
for col in one_hot_cols:
    df[col] = df[col].str.replace(",","")
    df[col] = df[col].str.split(" ").str[:]

df_dict = {elem : pd.DataFrame() for elem in one_hot_cols}
mlb = MultiLabelBinarizer()
for i, col in enumerate(df_dict.keys()):
    df_dict[col] = pd.DataFrame(mlb.fit_transform(df[col]),columns=class_list[i], index=df.link)

df = df.set_index("link")
for col in df_dict:
    df = df.join(df_dict[col]).drop("", axis=1)
    
df = df.drop(columns= one_hot_cols)
df["viertel"] = df.index.to_series().astype(str).str.extract(pat="(.*)\.\d\d\d\d\d\d")
df["viertel"] = df["viertel"].str.lower()
repl_viertel = ["--","\d\d\d\d\d","franfurter ","frankfurt am main","franfurt-am-main,""frankfurt-main-","frankfurtnord","frankfurt-","frankfurt","bei-frankfurt","naehe","sudlich-von","u-bahn-station-","-bei-ffm","1-minute-from-","am-main" ]
for word in repl_viertel:
    df["viertel"] = df["viertel"].str.replace(word,"")
df["viertel"] = df["viertel"].str.strip('-')

conditions = [
    (df["frei_ab"] == ""),
    ((df["frei_ab"] != "") & (df["frei_bis"] != "")),
    ((df["frei_ab"] != "") & (df["frei_bis"] == ""))]
choices = ['inaktiv', 'befristet', 'unbefristet']
df["status"] = np.select(conditions, choices)

df["direct_link"]= "https://www.wg-gesucht.de/wg-zimmer-in-Frankfurt-am-Main-" + df.index + ".html"

df["dauer"] = pd.to_datetime(df.frei_bis, format='%d.%m.%Y', errors='coerce') - pd.to_datetime(df.frei_ab, format='%d.%m.%Y', errors='coerce')
df['dauer'] = df['dauer'] / np.timedelta64(1, 'D')
df['dauer'].fillna(0, inplace=True)

df["wohnung"] = df["wohnung"].str.replace("\.","")
df["m2_pro_pers"] = pd.to_numeric(df['wohnung'], errors='coerce')/ pd.to_numeric(df['personen'], errors='coerce')

# Replace uncommon places with common places if they are included in common places
df["viertel_name"] = df.viertel.apply(replace_viertel, viertel_liste=freq_viertel)
df_mapped = df
df_mapped["baujahr"] = df_mapped.baujahr.str.extract(pat="baujahr (\d\d\d\d)")
df_mapped["PLZ"] = df_mapped.adresse.str.extract("(\d\d\d\d\d)")

# Replace uncommon PLZ with new value
# df_mapped.loc[~df_mapped["PLZ"].isin(list(pd.DataFrame(df.PLZ.value_counts()).query("PLZ > 10").index)), "PLZ"] = 99999

num_cols = ['wohnung', 'zimmergröße', 'sonstige', 'nebenkosten', 'miete', 'gesamtmiete', 'bus', 'männlich',
 'personen', 'weiblich', 'kaution', 'ablösevereinbarung', 'personen', 'bus', 'baujahr',"taeglich"]
for col in num_cols:
    df_mapped[col] = df_mapped[col].astype(str)
    df_mapped[col] = df_mapped[col].str.extract('(\d+)', expand=False)
    df_mapped[col] = df_mapped[col].astype(float)

print(df)