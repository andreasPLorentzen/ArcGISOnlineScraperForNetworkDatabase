import Neo4j_general_functions as Neo4j
from arcgis.gis import GIS
import datetime
import arcgis
import csv
import os
import pandas
import json
import requests

class Relationship:
    def __init__(self, From, To, properties={}):
        self.From = From
        self.To = To
        self.properties = properties
        self.propertyList = [properties[x] for x in self.properties.keys()]

    def __str__(self):
        a = str(self.From) + " --> " + self.To
        return a

    def __repr__(self):
        a = str(self.From) + " --> " + self.To
        return a

    def csv_list(self):
        return [self.From, self.To] + self.propertyList

class Item:
    def __init__(self, url="",
                 ownerNODE_ID = "",
                 ownerUsername = "",
                 type="unknown",
                 title="unknown",
                 homepage="",
                 geometry="",
                 size="",
                 numViews="",
                 usage={"7D":0,"30D":0,"60D":0,"6M":0,"1Y":0},
                 created_epoch=0,
                 lastModified_epoch=0,
                 sharedWith={},
                 isSubLayer="FALSE",
                 isInferred="FALSE",
                 source=False,
                 partOf=False,
                 uses=False,
                 tags=[],
                 orgType="unknown",
                 orgID="unknown",
                 orgUrl="unknown",
                 forgType="unknown",
                 forgID="unknown",
                 forgUrl="unknown",
                 snippet=""):
        if url == "" or url is None: # maps etc.
            self.NODE_ID = "ESRI_ITEM_" + str(homepage)
            self.url_to_object = homepage
        else:
            self.NODE_ID = "ESRI_ITEM_" + str(url)
            self.url_to_object = url
        self.NODE_ID = self.NODE_ID.upper()
        self.url = url
        if title == "":
            self.title="unknown"
        else:
            self.title=title
        self.ownerNODE_ID = ownerNODE_ID
        self.ownerUsername = ownerUsername
        self.type = type
        self.homepage = homepage
        self.geometry = geometry
        self.size_kb = 0
        try:
            self.size_kb = float(int(size)/1000)
        except:
            pass

        # org
        self.foundOn = {"orgType": forgType,"orgCode":forgID, "orgUrl":forgUrl}
        self.orgType = orgType
        self.orgID = orgID
        self.orgUrl = orgUrl
        self.orgNODE_ID = orgType + "_" + orgID

        # Time related
        # Time dependent
        self.str_time = "%Y-%m-%dT%H:%M:%S"
        try:
            self.created = datetime.datetime.fromtimestamp(created_epoch / 1000)
            self.created = datetime.datetime.strftime(self.created, self.str_time)
        except:
            self.created = "1970-01-01T01:00:00"


        try:
            self.LastModified = datetime.datetime.fromtimestamp(lastModified_epoch/1000)
            self.LastModified = datetime.datetime.strftime(self.LastModified, self.str_time)
        except:
            self.LastModified = "1970-01-01T01:00:00"

        # Usage related
        if numViews is None or numViews == "":
            self.numViews = "\'\'"
        else:
            self.numViews = int(float(numViews))

        self.usage_raw = usage
        self.usage_7D = int(usage["7D"])
        self.usage_30D = int(usage["30D"])
        self.usage_60D = int(usage["60D"])
        self.usage_6M = int(usage["6M"])
        self.usage_1Y = int(usage["1Y"])

        # Connections
        self.partOf = partOf #super.NODE_ID
        self.source = source
        if self.source != False:
                         #(url, title, visible, origin_NODE_ID, ConnectionType, datatype, id)
            self.source = (url, "",      "",    self.NODE_ID,   "SOURCE",       "",       "")

        self.uses = uses
        if self.uses != False:
            new_uses = []
            for row in uses:
                new_uses.append((row[0], row[1],      row[2],    self.NODE_ID,   row[4],       row[5],       row[6]))
            self.uses = new_uses
        self.tags = tags
        self.isSubLayer = isSubLayer
        self.isInferred = isInferred

        self.sharedWith_raw = sharedWith
        self.sharedWithDomain = []
        self.sharedWithGroups = []
        if self.sharedWith_raw is None or self.sharedWith_raw == "":
            self.sharedWithDomain = []
            self.sharedWithGroups = []
        else:
            try:
                self.sharedWithDomain, self.sharedWithGroups = self._fixShared(sharedWith)
            except:
                print("NBNBNB! could not fix shared", self.sharedWith_raw, self.url)


        # other not used:
        self.snippet = snippet
    def _fixShared(self, sharedDict):
        shared_temp_linesDomain = []
        shared_temp_linesGroups = []
        if "org" in sharedDict.keys():
                shared_temp_linesDomain.append(Relationship(From=self.NODE_ID.upper(),To=self.orgNODE_ID))

        if 'everyone' in sharedDict.keys():
            if sharedDict['everyone']:
                shared_temp_linesDomain.append(Relationship(self.NODE_ID.upper(),"PUBLIC_DOMAIN"))

        if "groups" in sharedDict.keys():
            if sharedDict["groups"] != []:
                for group in sharedDict["groups"]:
                    shared_temp_linesGroups.append(Relationship(From=self.NODE_ID.upper(), To="ESRI_GROUP_" + group.id))
        return shared_temp_linesDomain, shared_temp_linesGroups

    def csv_list(self):
        return [self.NODE_ID, self.title, self.type, self.geometry, self.url, self.homepage, self.created, self.LastModified, self.numViews, self.usage_7D, self.usage_30D, self.usage_60D, self.usage_6M, self.usage_1Y, self.isSubLayer, self.isInferred]

class Tag:
    def __init__(self, tag):
        self.NODE_ID = "TAG_" + tag
        self.tag = tag

    def csv_list(self):
        return [self.NODE_ID, self.tag]


todays_date = datetime.datetime.today()
usage_times = {
    # "24H": todays_date - datetime.timedelta(houers=24),
    "7D": todays_date - datetime.timedelta(days=7),
    "30D": todays_date - datetime.timedelta(days=30),
    "60D": todays_date - datetime.timedelta(days=60),
    "6M": todays_date - datetime.timedelta(days=182),  # 6 months
    "1Y": todays_date - datetime.timedelta(days=365),  # 1 year

}

# Misc functions
def getNewToken(org):
    request = org[2] + r"/sharing/generateToken?username=" + org[3] + "&password=" + org[4] + "&client=requestip&f=json"
    try:
        response = requests.get(request)
        # print("\t\ttrying to get token", vars(response))
        token = json.loads(str(response._content.decode("utf-8")))["token"].replace(".", "")
        # print(token)
    except:
        token = False
        print("\t\tcould not get token, logs inn for each item. This might make it go slower")

    return token

def importCSVusers(csvfile):
    return_table = []
    with open(csvfile,mode="r") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        first = True
        for row in reader:
            if not first:
                return_table.append({
                    "NODE_ID": row[0],
                    "username": row[1],
                    "orgNODE_ID":row[2]
                })
            first = False
    return return_table
    pass

def getUsage(item):
    tempusage = {
        # "24H": 0,
        "7D": 0,
        "30D": 0,
        "60D": 0,
        "6M": 0,
        "1Y": 0,
    }

    try:
        use = item.usage(date_range="1Y", as_df=True)
        if use.empty == False:

            for period in usage.keys():
                tempusage[period] = int(use[use["Date"] >= usage_times[period]]["Usage"].sum())
    except:
        print("Could not get usage data.")

    return tempusage

def getUses(item, token, Org):
    tempuses = []
    base_before_id = Org[2] + "/home/item.html?id="
    baseurl = Org[2]
    entities = {}


    # getting data from Rest API:
    if token != False:
        request = baseurl + "/sharing/rest/content/items/" + item.id + "/data?token=" + token
        # print(request)
    else:
        request = baseurl + "/sharing/rest/content/items/" + item.id + "/data?username=" + org[3] + "&password=" + org[4]
        # print(request)

    # GET RESPONSE
    try:
        response = requests.get(request)
        # print(vars(response))
        json_data = json.loads(str(response._content.decode("utf-8")))
    except:
        print("\t\t error in request from server. id: ", Items[type][item].id, " response: ", vars(response))


    # if webmap or web scene:
    #try:
    # Get top level layers
    try:
        for row in json_data["operationalLayers"]:

            # initiaise variables
            layerType, url, title, name, visibility, itemId = "", "", "", "", "", ""
            if "layerType" in row.keys():
                layerType = row["layerType"]
            if "url" in row.keys():
                url = row["url"]
            if "title" in row.keys():
                title = row["title"]
            if "name" in row.keys():
                name = row["name"]
            if "visibility" in row.keys():
                if row["visibility"] == True or row["visibility"] == "true" or row["visibility"] == "True":
                    visibility = "TRUE"
                else:
                    visibility = "FALSE"

            if "itemId" in row.keys():
                itemId = row["itemId"]

            # add to export
            if url != "":
                print("\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name,
                                                                                                                   title,
                                                                                                                   itemId, visibility,
                                                                                                                   url))
                # (url, title, visible, origin_NODE_ID, ConnectionType, datatype, id)
                tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))


            # for layers in groups
            if "layers" in row.keys():
                for layer in row["layers"]:
                    # initiaise variables
                    layerType, url, title, name, visibility, itemId = "", "", "", "", "", ""
                    if "layerType" in layer.keys():
                        layerType = layer["layerType"]
                    if "url" in layer.keys():
                        url = layer["url"]
                    if "title" in layer.keys():
                        title = layer["title"]
                    if "name" in layer.keys():
                        name = layer["name"]
                    if "visibility" in layer.keys():
                        visibility = layer["visibility"]
                    if "itemId" in layer.keys():
                        itemId = layer["itemId"]
                    # add to export
                    if url != "":
                        print(
                            "\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType,
                                                                                                                           name,
                                                                                                                           title,
                                                                                                                           itemId,
                                                                                                                           visibility,
                                                                                                                           url))
                        tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        print("\t\t\tLayers:", len(tempuses))
        return tempuses
    except:
        pass


    # if app
    try:
        for row in json_data["dataSources"].keys():
            print("\t\t\t\t", row, json_data["dataSources"][row])


            layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
            url = base_before_id + json_data["dataSources"][row]
            itemId = json_data["dataSources"][row]

            # add to export
            print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                               itemId,
                                                                                                               visibility, url))

            tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses
    except:
        pass


    # if Web mapping application
    try:
        for row in json_data["links"]:
            layerType, url, title, name, visibility, itemId = "Hyperlink", "", "", "", "", ""
            url = row["url"]
            itemId = json_data["dataSources"][row]

            print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                               itemId, visibility,
                                                                                                               url))
            tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))

    except:
        pass
    # map part...
    try:
        layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
        url = base_before_id + json_data["map"]["itemId"]
        itemId = json_data["map"]["itemId"]

        # add to export
        print("\t\t\t\t  -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title, itemId,
                                                                                                       visibility, url))
        tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses
    except:
        pass


    # another app type Editor
    try:
        layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
        url = base_before_id + json_data["values"]["webmap"]
        itemId = json_data["values"]["webmap"]
        print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                       itemId,
                                                                                                       visibility, url))
        tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses
    except:
        pass


    # dashboard
    try:
        for row in json_data["widgets"]:
            if row["type"] == "mapWidget":
                layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
                url = base_before_id + row["itemId"]
                itemId = row["itemId"]
                print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                               itemId,
                                                                                                               visibility, url))
                tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses

    except:
        pass

    # storymaps
    try:
        for node in json_data["nodes"]:
            # print(node)
            if json_data["nodes"][node]["type"] == "webmap":
                layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
                url = base_before_id + json_data["nodes"][node]["data"]["map"].split("-")[1]
                itemId = json_data["nodes"][node]["data"]["map"].split("-")[1]

                print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                               itemId,
                                                                                                               visibility, url))
                tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses
    except:
            pass

    # old storymap
    try:
        for row in json_data["values"]["story"]["sections"]:
            if row["media"]["type"] == "webmap":
                layerType, url, title, name, visibility, itemId = "Web Map", "", "", "", "", ""
                url = base_before_id + row["media"]["webmap"]["id"]
                itemId = row["media"]["webmap"]["id"]
                print("\t\t\t\t -> layerType: {0} name: {1} title: {2} itemId: {3} visibility: {4} url: {5} ".format(layerType, name, title,
                                                                                                               itemId,
                                                                                                               visibility, url))
                tempuses.append((url, title, str(visibility), "", "USES",layerType, itemId))
        return tempuses

    except:
        pass

    print("type not recognized.")
    print(vars(json_data))

    return False

def printToCSV(data,name,folder,csv_header):
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
                else:
                    w_list.append(str(element).replace("\n", " <br/> ").replace(",","|"))
            writer.writerow(w_list)

def generateCypherCode(csv_name="",header=[],property_types=[]):
    returnLines = []
    returnLines.append("LOAD CSV WITH HEADERS FROM 'file:///" + csv_name + ".csv' as csv")

    filename_parts = csv_name.split("_")
    creator = filename_parts[0]
    type = filename_parts[1]
    subtype = ""

    if type == "NODE":
        Node_type = filename_parts[2]
        try:
            for n in filename_parts[3:]:
                Node_type += "_" + n
        except:
            pass

        if "@" in Node_type:
            subtype = "subtype:'" + Node_type.split("@")[-1] + "', "
            Node_type = Node_type.split("@")[0]

        property_string = "{NODE_ID:csv.NODE_ID, " + subtype


        for row in zip(header, property_types):
            if row[0] != "NODE_ID":
                if row[1] == "str":
                    property_string += row[0] + ":csv." + row[0] + ", "
                elif row[1] == "int":
                    property_string += row[0] + ":toInteger(csv." + row[0] + "), "
                elif row[1] == "float":
                    property_string += row[0] + ":toFloat(csv." + row[0] + "), "
                elif row[1] == "bool":
                    property_string += row[0] + ":toBoolean(csv." + row[0] + "), "
                elif row[1] == "Datetime":
                    property_string += row[0] + ":datetime(csv." + row[0] + "), "

        property_string = property_string[:-2]
        returnLines.append("MERGE(u:" + Node_type +" "+ property_string + "})")
        returnLines.append("")
        returnLines.append(";")
        returnLines.append("")

    if type == "REL":
        From_node_type = filename_parts[2]
        To_node_type = filename_parts[3]
        Relation_type = filename_parts[4]
        try:
            for x in filename_parts[5:]:
                Relation_type +="_" + x
        except:
            pass

        property_string = " {" + subtype
        i = 1
        for row in zip(header,property_types):
            if i > 2:
                if row[1] == "str":
                    property_string += row[0] + ":csv." + row[0] + ", "
                elif row[1] == "int":
                    property_string += row[0] + ":toInteger(csv." + row[0] + "), "
                elif row[1] == "float":
                    property_string += row[0] + ":toFloat(csv." + row[0] + "), "
                elif row[1] == "bool":
                    property_string += row[0] + ":toBoolean(csv." + row[0] + "), "
                elif row[1] == "DateTime":
                    property_string += row[0] + ":datetime(csv." + row[0] + "), "

            i += 1
        if property_string != " {":
            property_string = property_string[:-2]
        property_string += "}"
        if property_string == " {}":
            property_string = ""

        returnLines.append("MERGE (f:" + From_node_type + " {NODE_ID:csv.From})")
        returnLines.append("MERGE (t:" + To_node_type + " {NODE_ID:csv.To})")
        returnLines.append("MERGE (f) -[:" + Relation_type + property_string + "]-> (t)")
        returnLines.append(" ")
        returnLines.append(";")
        returnLines.append(" ")


    for row in returnLines:
        print(row)

    returnLines = returnLines[:-2]
    return returnLines


# import users
Users = importCSVusers(os.path.join(Neo4j.temp_folder, "Users.csv"))

Items = {}
Tags = {}

Item_User_OWNER = {}
Item_Item_PART_OF = {}
Item_Item_USES = {}
Item_Item_SOURCE = {}
Item_Group_SHARED_WITH = {}
Item_Tag_HAS = {}

Item_AccessDomain_ON = {}
Item_AccessDomain_SHARED_WITH = {}

# for org:
for org in Neo4j.organizations:
    org_name = org[0]
    org_code = org[1]
    org_type = org[5]
    org_NODE_ID = org_type + "_" + org_code
    users_on_org = []
    print("\t Working width ", org[2], org_type + "_" + org_code)
    for row in Users:
        if row["orgNODE_ID"] == org_NODE_ID:
            users_on_org.append(row)
    number_of_users = len(users_on_org)
    print("\t\tUsers on org:", len(users_on_org))

    # Get access_token
    # for user in org which is not != outside user:

    if number_of_users != 0:
        gis = GIS(org[2], username=org[3], password=org[4])

        token_time = datetime.datetime.now()
        token = getNewToken(org)
        user_number = 1
        for user in users_on_org:
            print(str(user_number) + "/" + str(number_of_users) + "\tWorking with user: ", user["username"])
            user_number +=1
            quary = "owner:" + user["username"]

            items = arcgis.gis.ContentManager(gis)
            allItems = items.search(quary, max_items=Neo4j.max_results)
            print("\t\t\tUser has ", len(allItems), " items.")
            i = 1
            for item in allItems:
                print("\t\t\t", i, item.url, item.homepage)
                i+=1
                timedelta = datetime.datetime.now() - token_time
                if int(timedelta.seconds) > 6000:# (100 min, 120 = levetid)
                    token = getNewToken(org)
                    token_time = datetime.datetime.now()
                # Getting properties for base item
                try:

                    title = "unknown"
                    try:
                        title = item.title
                    except:
                        pass

                    # usage
                    usage = {
                        # "24H": 0,
                        "7D": 0,
                        "30D": 0,
                        "60D": 0,
                        "6M": 0,
                        "1Y": 0,
                    }
                    try:
                        if Neo4j.get_usage:
                            usage = getUsage(item)
                    except:
                        print("error in getting Usage.")


                    # source:
                    source = False
                    if "sourceUrl" in item.keys():
                        source = item.sourceUrl

                    # layers
                    layers = False
                    if "layers" in item.keys():
                        layers = item.layers

                    # size
                    size=""
                    try:
                        size = item.size
                    except:
                        pass

                    # tags
                    tags = []
                    try:
                        tags = item.tags
                    except:
                        pass

                    # created
                    created = 0
                    try:
                        created = item.created
                    except:
                        pass

                    # created
                    LastModified = 0
                    try:
                        LastModified = item.modified
                    except:
                        pass


                    # shared with
                    shared_with = False
                    try:
                        shared_with = item.shared_with
                    except:
                        pass

                    #item.numViews
                    numViews =""
                    try:
                        numViews = item.numViews
                    except:
                        pass

                    #snippet:
                    snippet = ""
                    try:
                        snippet = item.snippet
                    except:
                        pass

                    # type
                    type = "unknown"
                    try:
                        type = item.type
                    except:
                        pass

                    #homepage
                    homepage = ""
                    try:
                        homepage = item.homepage
                    except:
                        pass

                except:
                    print("something failed in getting information about Item.")

                # gather uses if if item is used
                uses = False
                try:
                    if type in ["Web Map", "Web Scene","Web Mapping Application"]:
                        uses = getUses(item, token, org)
                except:
                    pass

                # importing main node
                try:
                    temp_item = Item(url=item.url,
                                    ownerNODE_ID=user["NODE_ID"],
                                    ownerUsername=user["username"],
                                    type=type,
                                    title=title,
                                    homepage=homepage,
                                    size=size,
                                    numViews=numViews,
                                    usage=usage,
                                    created_epoch=created,
                                    lastModified_epoch=LastModified,
                                    sharedWith=shared_with,
                                    source=source,
                                    tags=tags,
                                    uses=uses,
                                    forgType=org[5],
                                    forgUrl=gis.url,
                                    forgID=org[1],
                                    orgType=org[5],
                                    orgUrl=org[2],
                                    orgID=org[1])


                    Items[temp_item.NODE_ID] = temp_item
                except:
                    print("could not add item to nodes")

                # Getting layers:
                if layers != False and layers != [] and layers != None:
                    for layer in layers:
                        try:
                            print("\t\t\t\t'-->", layer.url)
                            layer_url = layer.url
                            if layer.url != item.url:
                                # title
                                layertitle = "Sublayer of " + title
                                try:
                                    layertitle = layer.title
                                except:
                                    pass


                                # modified
                                modified = LastModified
                                try:
                                    modified = layer.properties["editingInfo"]["lastEditDate"]
                                except:
                                    pass

                                # geometry
                                geometry = ""
                                try:
                                    geometry = layer.properties["geometryType"]
                                except:
                                    pass

                                # type
                                layertype = "unknown"
                                # works if its from Feature service layer
                                try:
                                    layertype = layer.properties["type"]
                                except:
                                    pass

                                # works for some other datasets
                                try:
                                    layertype = layer._lazy_properties["type"]
                                except:
                                    pass

                                layer_temp_item = Item(url=layer_url,
                                                 ownerNODE_ID=user["NODE_ID"],
                                                 ownerUsername=user["username"],
                                                 type=layertype,
                                                 title=layertitle,
                                                 homepage=homepage,
                                                 size=size,
                                                 numViews=numViews,
                                                 usage=usage,
                                                 created_epoch=created,
                                                 lastModified_epoch=modified,
                                                 sharedWith=shared_with,
                                                 source=source,
                                                 tags=tags,
                                                 uses=uses,
                                                 partOf=temp_item.NODE_ID,
                                                 isSubLayer="TRUE",
                                                 geometry=geometry,
                                                 forgType=org[5],
                                                 forgUrl=gis.url,
                                                 forgID=org[1],
                                                 orgType=org[5],
                                                 orgUrl=org[2],
                                                 orgID=org[1])

                                Items[layer_temp_item.NODE_ID] = layer_temp_item
                        except:
                            print("\t\t\t\t ==> ERROR: COULD NOT ADD LAYER")


# Getting inferred Items and uses:
lookup_items_url = {}
for item in Items:
    if Items[item].url_to_object.upper() in lookup_items_url.keys():
        print("Ehm. denne finnes alt:", Items[item].url_to_object)
    else:
        lookup_items_url[Items[item].url_to_object] = item

# find all refereced items
referenced_items = []
for item in Items:
    if Items[item].uses != False and Items[item].uses !=[]:
        for used in Items[item].uses:
            referenced_items.append(used)
    if Items[item].source != False:
        referenced_items.append(Items[item].source)

i = 0
for referenced in referenced_items:
    i += 1
    #(url, title, visible, origin_NODE_ID, ConnectionType, datatype, id)
    if referenced[0] in lookup_items_url.keys():
        if referenced[4] == "USES":
            Item_Item_USES[str(i)] = Relationship(From=referenced[3].upper(),To=lookup_items_url[referenced[0]].upper(),properties={"title":referenced[1],"visable":referenced[2]})
        elif referenced[4] == "SOURCE":
            Item_Item_SOURCE[str(i)] = Relationship(From=referenced[3].upper(),To=lookup_items_url[referenced[0]].upper())
    else:
        # create new inferred item
        temp_item = Item(url=referenced[0],type=referenced[5],isInferred="TRUE",title=referenced[1])
        Items[temp_item.NODE_ID] = temp_item
        lookup_items_url[temp_item.url_to_object] = temp_item.NODE_ID
        # and add the connection.
        if referenced[4] == "USES":
            Item_Item_USES[str(i)] = Relationship(From=referenced[3].upper(), To=temp_item.NODE_ID.upper(),
                                                  properties={"title": referenced[1], "visable": referenced[2]})
        elif referenced[4] == "SOURCE":
            Item_Item_SOURCE[str(i)] = Relationship(From=referenced[3].upper(), To=temp_item.NODE_ID.upper())



# print("\n"*5, "Inferred")
# for row in Items.keys():
#     if Items[row].isInferred == "TRUE":
#         print(vars(Items[row]))




# Generate owner, part of, sharedWith grups and domains
i = 0
for item in Items.keys():
    if Items[item].partOf != False:
        Item_Item_PART_OF[str(i)] = Relationship(From=item.upper(), To=Items[item].partOf.upper())

    if Items[item].isInferred == "FALSE":
        Item_AccessDomain_ON[str(i)] = Relationship(From=item.upper(), To=Items[item].orgNODE_ID)
        Item_User_OWNER[str(i)] = Relationship(From=item.upper(), To=Items[item].ownerNODE_ID)

    b = 0
    if Items[item].sharedWithDomain != []:
        for row in Items[item].sharedWithDomain:
            Item_AccessDomain_SHARED_WITH[str(i)+ "_" + str(b)] = row
            b += 1

    b = 0
    if Items[item].sharedWithGroups != []:
        for row in Items[item].sharedWithGroups:
            Item_Group_SHARED_WITH[str(i) + "_" + str(b)] = row
            b += 1

    b = 0
    if Items[item].tags != [] and Items[item].tags != "" and Items[item].tags is not None:
        for row in Items[item].tags:
            if row not in Tags.keys():
                temp_tag = Tag(tag=row)
                Tags[row] = temp_tag
                Item_Tag_HAS[str(i) + "_" + str(b)] = Relationship(From=item.upper(), To=temp_tag.NODE_ID)
                b += 1
    i += 1


# Exporting
print("Printing CSV files for use in Neo4j")
list_of_csv_exports = [
    # Nodes
    # Dataset       Name of CSV      Header
    [Items, "ADLR_NODE_Item", ["NODE_ID", "title", "type", "geometry", "url", "homepage", "created", "LastModified", "numViews", "usage_7D", "usage_30D", "usage_60D", "usage_6M", "usage_1Y", "isSubLayer", "isInferred"],["str", "str", "str", "str", "str", "str", "Datetime", "Datetime", "int", "int", "int", "int", "int", "int", "bool", "bool"]],
    [Tags,"ADLR_NODE_Tag_from_items",["NODE_ID","tag"],["str","str"]],

    # Relations
    [Item_Item_SOURCE,"ADLR_REL_Item_Item_SOURCE",["From","To"],["str","str"]],
    [Item_Item_USES,"ADLR_REL_Item_Item_USES",["From","To", "title", "visible"],["str","str"]],
    [Item_Item_PART_OF,"ADLR_REL_Item_Item_PART_OF",["From","To"],["str","str"]],

    [Item_AccessDomain_SHARED_WITH,"ADLR_REL_Item_AccessDomain_SHARED_WITH",["From","To"],["str","str"]],
    [Item_AccessDomain_ON,"ADLR_REL_Item_AccessDomain_ON",["From","To"],["str","str"]],

    [Item_Group_SHARED_WITH,"ADLR_REL_Item_Group_SHARED_WITH",["From","To"],["str","str"]],
    [Item_Tag_HAS,"ADLR_REL_Item_Tag_HAS",["From","To"],["str","str"]],

    [Item_User_OWNER,"ADLR_REL_Item_User_OWNER",["From","To"],["str","str"]],

]

for csvfile in list_of_csv_exports:
    printToCSV([csvfile[0][x].csv_list() for x in csvfile[0].keys()], csvfile[1], Neo4j.export_folder, csvfile[2])
    print("\t", csvfile[1], " created.")

cypher_import_code = []
output_file_name = "cypher_code.txt"
for csvfile in list_of_csv_exports:
    cypher_import_code += generateCypherCode(csv_name=csvfile[1],header=csvfile[2],property_types=csvfile[3])

with open(os.path.join(Neo4j.export_folder, output_file_name), "w") as cypherFile:
    writer = csv.writer(cypherFile)
    for line in cypher_import_code:
        cypherFile.write(line)


