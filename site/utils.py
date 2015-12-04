from time import strptime
import numpy as np
path = "/home/dharmahound/analytics.andrewgarrettreece.com/data/"

def set_debate_schedule():
    try:
        from bs4 import BeautifulSoup as bs
        import requests, json, re
        from time import strptime
        import numpy as np

        url = "https://www.washingtonpost.com/graphics/politics/2016-election/debates/schedule/"
        html = requests.get(url).text
        soup = bs(html, "html.parser")
        events = [['party','date','time','datetime','caveat']]
        items = soup.find_all(class_="debate-schedule-item")

        for item in items:
            party = item.find(class_="party").string.split(" ")[0]
            date = re.sub("\\.|,","",item.find(class_="date").string)
            time_str = item.find("span", class_="label", text=re.compile("Time"))
            if time_str: 
                caveat = ""
                rgx = re.search("(\d{1,2}\\:?(\d{2})?)\sp\\.m\\.",time_str.next_sibling)
                if rgx:
                    time = rgx.group(1)
                    if len(time) <= 2:
                        time = str(int(time)+12)+":00"
                    else:
                        hr,mins = time.split(":")
                        hr = str(int(hr)+12) # always at night
                        time = hr + ":" + str(mins)
            else:
                caveat = "The time for this debate has not yet been officially set. The time given here is an approximation based on previous debate schedules."
                time = "20:00"
                hr,mins = time.split(":")
            try: 
                mon, day, yr = date.split(" ")
                mon = str(strptime(mon[:3],'%b').tm_mon)
                if len(mon) == 1:
                    mon = "0"+mon
                if len(day) == 1:
                    day = "0"+day
                timestamp = yr+'-'+mon+'-'+day+"T"+hr+":"+mins+":00"
                events.append( [party,date,time,timestamp,caveat] )
            except:
                pass
                    
        np.savetxt(path+"events.csv",events,fmt='"%s"',delimiter=",") # inner double quotes make strings quoted in csv
    except Exception, e:
        return str(e)

def get_debate_schedule():
    with open(path+"events.csv") as fp:
        csv = fp.read()
    return csv 




