from urllib.request import urlopen
from bs4 import BeautifulSoup
from bs4 import NavigableString
import requests
import pandas as pd
import re
import collections
import time
from sklearn.preprocessing import MultiLabelBinarizer
import os
import sys
import zipfile
import glob
from zipfile import ZipFile
from api import api

source_dir = r'data\raw'

def zip_files(source_dir):
    for filename in os.listdir(source_dir):
        if filename.endswith('.zip'):
            yield filename
            
def clean_string2(liste):
    #liste = Flatten(liste)
    #liste = " ".join(liste)
    liste =  re.sub(r"[\W\_]|\d+", ' ', liste)
    liste = " ".join(liste.split())
    liste = liste.lower()
    
    return liste

def Flatten(ul):
    fl = []
    for i in ul:
        if type(i) is list:
            fl += Flatten(i)
        else:
            fl += [i]
    return fl

def get_main_text(soup):
    text = soup.find(id="ad_description_text").text
    text = clean_string2(text)
    cut_string = "html ad container nextsibling"
    try:
        text = text.split(cut_string,1)[1] 
    except:
        pass
    return text


def get_text_from_clean(text, liste, direction= "right"):

    pairs = []
   
    if direction == "right":
        for item in liste:
            try:
                if item in text:
                    pairs.append([item, text.split(item)[1].split()[0]])
                else:
                    pairs.append([item, "none"])
            except:
                pairs.append([item, "none"])
    if direction == "left":
        for item in liste:
            try:
                if item in text:
                    pairs.append([item, text.split(item)[0].split()[-1]])
                else:
                     pairs.append([item, "none"])   
            except:
                pairs.append([item, "none"])
    
    return pairs

def clean_string(liste):
    liste = Flatten(liste)
    liste = " ".join(liste)
    liste = " ".join(liste.split())
    return liste

def get_text_from_html(bs, class_name):
    string_list =[]
    soup = bs.find_all(class_=class_name)
    for entry in soup:
        string_list.append(entry.text)
    return string_list

def Flatten(ul):
    fl = []
    for i in ul:
        if type(i) is list:
            fl += Flatten(i)
        else:
            fl += [i]
    return fl


def get_bs_from_html(html):
    return BeautifulSoup(html.text, "html.parser")

def get_bs_from_http(link):
    html = requests.get(link)
    return BeautifulSoup(html.text, "html.parser")

def get_html_request(link):
    return requests.get(link)

def get_html_from_scraper(link, api=api):
    payload = {'api_key': api, 'url':link}
    return requests.get('http://api.scraperapi.com', params=payload)

def get_bs_from_http_scraper(link, api =api):
    payload = {'api_key': api, 'url':link}
    html = requests.get('http://api.scraperapi.com', params=payload)
    return BeautifulSoup(html.text, "html.parser")


def get_all_links_from_site(bs):
    try:
        all_links = []
        classes = ["listenansicht1 listenansicht-inactive offer_list_item","listenansicht0 listenansicht-inactive offer_list_item",
                  "listenansicht0 offer_list_item", "listenansicht1 offer_list_item"]
        
        for scrape_class in classes:
            links = bs.findAll(class_=scrape_class)
            for link in links:
                all_links.append(link["adid"][31:])
    except:
        print("something went wrong with get_all_links")
    return all_links

def get_all_links(nr_min_sites = 0, nr_max_sites = 0, sleep_time =0, scraper = get_bs_from_http_scraper):
    linklist = []
    for i in range(nr_min_sites,nr_max_sites):
        try:
            url = 'https://www.wg-gesucht.de/wg-zimmer-in-Frankfurt-am-Main.41.0.0.'+ str(i) +'.html'
            linklist.extend(get_all_links_from_site(scraper(url)))
            print(f"{i+1-nr_min_sites} from {nr_max_sites-nr_min_sites} Pages loaded. Thats {(i-nr_min_sites+1)/(nr_max_sites-nr_min_sites):.2%}.\
            Linklist now has {len(linklist)} rows (expexted {(i+1-nr_min_sites)*20})", end='\r')        
            time.sleep(sleep_time)
        except:
            pass
    return linklist   

def merge_dicts(dic1,dic2):
    try:
        dic3 = dict(dic2)
        for k, v in dic1.items():
            dic3[k] = Flatten([dic3[k], v]) if k in dic3 else v
        return dic3
    except:
        return dic1

def csv_files(source_dir):
    for filename in os.listdir():
        if filename.endswith('.txt'):
            yield filename

def zip_files(source_dir):
    for filename in os.listdir(source_dir):
        if filename.endswith('.zip'):
            yield filename

def get_bot_and_outdated_links(source_dir=source_dir):
    deleted_txt = "Vermutlich ist das Objekt bereits vergeben."
    cap_txt ="dass kein Bot die Website automatisiert aufruft."

    theFiles = list(os.listdir(source_dir))

    file_list_del = []
    file_list_bot =[]
    theDict = dict()
    for i in theFiles: #Calculate size for all files here. 
        theStats = os.stat(source_dir + "\\"+i)
        theDict[i] = theStats
        if theDict[i].st_size < 85000 and theDict[i].st_size > 60000:
            f = source_dir + "\\"+ i
            with open(f, "r") as file:
                a = file.read()
                if (deleted_txt in a):
                    file_list_del.append(i[:-4])
                if (cap_txt in a):
                    file_list_bot.append(i[:-4])

    for item in file_list_del:
        f = source_dir + "\\"+ item + ".txt"
        os.remove(f)

    for item in file_list_bot:
        f = source_dir + "\\"+ item + ".txt"
        os.remove(f)    
    
    try:
        t1 = pd.read_csv(r'data\Outdated_links.csv')
    except:
        t1 = pd.DataFrame({'Links' : []})
    t2 = pd.DataFrame(file_list_del, columns =['Links'])
    t1 = pd.concat([t1,t2]).drop_duplicates(keep="first")
    t1.to_csv(r'data\Outdated_links.csv', index=False)

    try:
        t1 = pd.read_csv(r'data\Bot_links.csv')
    except:
        t1 = pd.DataFrame({'Links' : []})
        
    t2 = pd.DataFrame(file_list_bot, columns =['Links'])
    t1 = pd.concat([t1,t2]).drop_duplicates(keep="first")
    t1.to_csv(r'data\Bot_links.csv', index=False)

def zip_my_files(orig_dir, source_dir=source_dir):
    try:
        source_dir = source_dir
        dest_dir = source_dir
        os.chdir(dest_dir)  # To work around zipfile limitations
        for csv_filename in csv_files(source_dir):
            file_root = os.path.splitext(csv_filename)[0]
            zip_file_name = file_root + '.zip'
            zip_file_path = os.path.join(dest_dir, zip_file_name)
            with zipfile.ZipFile(os.getcwd()+"\\"+zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(csv_filename)
            os.remove(os.getcwd()+"\\"+csv_filename)
    except:
        print("no files found")
    # change wd back
    os.chdir(orig_dir)   
        
def get_housing_links():
    
    get_bot_and_outdated_links()
    zipped_files = []
    for csv_filename in zip_files(source_dir):
        zipped_files.append(csv_filename[:-4])
    zipped_files = pd.DataFrame(zipped_files, columns =['Links'])
    zipped_files["Links"] = zipped_files["Links"].str.lower()

    house_links = pd.read_csv(r'data\All_Housing_Links.csv')
    house_links["Links"] = house_links["Links"].str.lower()
    outdated_links = pd.read_csv(r'data\Outdated_links.csv')
    outdated_links["Links"] = outdated_links["Links"].str.lower()
    
    house_links["Links"] = house_links["Links"].str.slice(0,-5)
    house_links = pd.concat([house_links["Links"],outdated_links["Links"],zipped_files["Links"] ])
    
    house_links = house_links.drop_duplicates(keep=False)
    return house_links        



def get_all_data_from_site(bs, link):
      
    names = ["Wohnung","Zimmergröße","Sonstige","Nebenkosten","Miete","Gesamtmiete","Kaution","Ablösevereinbarung"]
    my_list = get_text_from_html(bs,"col-sm-12 hidden-xs")
    my_list = clean_string(my_list)
    dict1 = dict(get_text_from_clean(my_list,names,"left"))
       
    names = ["frei ab: ", "frei bis: "]
    my_list = get_text_from_html(bs,"col-sm-3")
    my_list = clean_string(my_list)
    dict2 = dict(get_text_from_clean(my_list,names,"right"))     
    
    item = "headline headline-detailed-view-title"
    lookups = ["weiblich", "Malmännliche", "gesucht"]
    count = []
    for lookup in lookups:
        try:
            people_list = bs.find(class_=item).find_all(class_="vis")
            count.append(str(people_list).count(lookup))
        except:
            count.append("none")
    dict4 = dict(zip(["weiblich","männlich", "gesucht"], count))
      
    
    my_list = get_text_from_html(bs,"ul-detailed-view-datasheet print_text_left")
    my_list = [x.strip() for x in my_list]
    try:
        dict5 = dict(get_text_from_clean(my_list[1],["zwischen"],"left"))
    except:
        dict5 = dict(get_text_from_clean(my_list,["zwischen"],"left"))
        
                                     
    my_list = get_text_from_html(bs,"ul-detailed-view-datasheet print_text_left")
    my_list = [x.strip() for x in my_list]
    try:
        dict8 = dict(get_text_from_clean(my_list[1],["Geschlecht"],"right"))
    except:
        dict8 = dict(get_text_from_clean(my_list,["Geschlecht"],"right"))

    
    item_list = [
    "mdi mdi-shower mdi-36px noprint",
    "mdi mdi-folder mdi-36px noprint",
    "mdi mdi-bed-double-outline mdi-36px noprint",
    "mdi mdi-city mdi-36px noprint",
    "mdi mdi-wifi mdi-36px noprint",
    "mdi mdi-monitor mdi-36px noprint",
    "mdi mdi-layers mdi-36px noprint",
    "mdi mdi-car mdi-36px noprint",
    "mdi mdi-silverware-fork-knife mdi-36px noprint",
    "mdi mdi-office-building mdi-36px noprint",
    "mdi mdi-bus mdi-36px noprint"
    ]
    data_list = []
    for item in item_list:
        try:
            data_list.append([item.split("-")[1].split()[0], clean_string([bs.find(class_=item).next_sibling.next_sibling.next_sibling])])
        except:
            data_list.append([item.split("-")[1].split()[0],"none"])
    dict6 = dict(data_list)
    
    liste= get_text_from_html(bs,"col-sm-4 mb10")
    adress_string = clean_string(liste).replace("Adresse ","").replace("Umzugsfirma beauftragen1","").replace("Umzugsfirma beauftragen 1","")
    dict7 = {"Adresse":adress_string, "Link": link}
    
    names = "Miete pro Tag: "
    my_list = get_text_from_html(bs,"col-sm-5")
    my_list = clean_string(my_list)
    if names in my_list:
        dict9 = {"taeglich":1}
    else:
        dict9 = {"taeglich":0}
    
    div_id = 'popover-energy-certification'
    try:
        cs = clean_string([bs.find(id=div_id).next_sibling])
        dict10 = {"baujahr" : cs}
    except:
        dict10 = {"baujahr" : "none"}
    
    nichtrauchen= "Rauchen nicht erwünscht"
    rauchen = "Rauchen überall erlaubt"
    
    my_list = get_text_from_html(bs,"col-sm-6")
    my_list = clean_string(my_list)
    if rauchen in my_list:
        dict11 = {"rauchen":"raucher"}
    if nichtrauchen in my_list:
        dict11 = {"rauchen":"nichtraucher"}
    if rauchen not in my_list and nichtrauchen not in my_list:
        dict11 = {"rauchen":"keine_Angabe"}
    
    wg_list = ["Zweck-WG","keine Zweck-WG","Berufstätigen-WG", "gemischte WG","Studenten-WG","Frauen-WG","Azubi-WG"]
    dict12 = []
    for wg in wg_list:
        my_list = get_text_from_html(bs,"col-sm-6")
        my_list = clean_string(my_list)
        if wg in my_list:
            dict12.append([wg,1])
        else:
            dict12.append([wg,0])
    dict12 = dict(dict12)

    dict_list =[dict1,dict2,dict4,dict5,dict8, dict6,dict7,dict7,dict9,dict10, dict11, dict12]
    for item in dict_list:
        dict1.update(item)
    return dict1