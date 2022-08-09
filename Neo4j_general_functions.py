logfile = []


# input data
'''Organization on the form:
"Node_name", code, URL (the imporant stuff), username, password, name
'''

organizations = [
    ("NAME",   "XQb5TfnBnLwbfWV", "https://company.maps.arcgis.com",             "username",       "password", "ArcGIS_Online"),
    ("PORTAL_NAME", "012356789ABCDEF", "https://arcgisportal.company.com/portal",     "username",        "password", "ArcGIS_Enterprise_server"),
    ("AGOL_NAME",   "DIcffHalljSYvfk", "https://ntnu-gis.maps.arcgis.com",         "username",   "password", "ArcGIS_Online")
        ]


input_folder = r""
export_folder = r"C:\00_TEMP\NEO4J\EXPORT"
temp_folder = r"C:\00_TEMP\NEO4J\TEMP"

query = ""
max_results =200
get_usage = True


def printToCSV(data,name,folder,csv_header):
    import os
    import csv
    file = os.path.join(folder,name+".csv")
    with open(file, 'w',encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",",lineterminator='\n')
        writer.writerow(csv_header)
        for row in data:
            #Fix for Neo4j
            #print(row)
            w_list = []
            for element in row:
                if element == "" or element is None:
                    w_list.append("\' \'")
                elif type(element) == "str":
                    w_list.append(element.replace("\n", " <br/> ").replace(",","|"))
                else:
                    try:
                        w_list.append(element.replace("\n"," <br/> ").replace(",","|"))
                    except:
                        w_list.append(element)
            writer.writerow(w_list)