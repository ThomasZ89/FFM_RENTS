from scrape_funcs import *

sleep_time = 0
scraper = get_bs_from_http_scraper


for i in range(300,350,5):
    ##### Load old links
    try:
        old_links = pd.read_csv(r'data\All_Housing_Links.csv', usecols=["Links"])
    except:
        old_links = pd.DataFrame({'Links' : []})


    ##### Scrape new links
    new_links = get_all_links(nr_min_sites = i,nr_max_sites= i+5, sleep_time = sleep_time, scraper= scraper)
    new_links = pd.DataFrame(new_links, columns=["Links"])
    #### Combine and save
    combined = pd.concat([old_links, new_links]).drop_duplicates(keep="first")
    combined.to_csv (r'data\All_Housing_Links.csv', index = False, header=True)
