from scrape_funcs import *

sleep_time = 0
pages = 100

# reset current wd if needed
# os.chdir(r'c:/Users/thomas.zoellinger/PycharmProjects/rent')

zip_my_files(orig_dir=r'c:/Users/thomas.zoellinger/PycharmProjects/rent')

house_links = get_housing_links()

cap_txt ="Alle Webseiten nutzen daher Captchas, um zu prüfen, ob beispielsweise ein Formular"
cap_txt2 = "Vermutlich ist das Objekt bereits vergeben."
cap_txt3 ="Wenn Sie glauben, dass dies ein Fehler im System ist, dann schreiben Sie doch bitte eine kurze Nachricht"

for i in range(pages):
    if len(house_links) < 1:
        break
    name = house_links.iloc[i]
    stem = "https://www.wg-gesucht.de/wg-zimmer-in-Frankfurt-am-Main-"
    link =  stem + name + ".html"
    path = f"data\\raw\\\\{name}.txt"
    time.sleep(sleep_time)
    try:
        r = get_html_from_scraper(link)
        if cap_txt in r.text:
            print("bot",  end='\r')
        elif cap_txt2 in r.text:
            print("vergeben",  end='\r')
        elif cap_txt3 in r.text:
            print("nicht gefunden",  end='\r')
        else:
            print(link,  end='\r')
        with open(path, 'w', encoding="utf-8") as file:
            file.write(r.text) 
    except:
        print("error",  end='\r')
    
zip_my_files(orig_dir=r'c:/Users/thomas.zoellinger/PycharmProjects/rent')

print("finished")