import Neo4j_general_functions as Neo4j

print("Starting script...")
print("Importing packages...")
from arcgis.gis import GIS
import datetime
import arcgis
import csv
import os
import pandas
import json
import requests

#print(Neo4j.organizations)


class User:
    def __init__(self, username="", email="unknown", fullName="unknown", userRole="unknown",userLevel="unknown", userID="unknown", created_epoch=0, lastLogin_epoch=0, orgType="unknown",orgID="unknown",orgUrl="unknown", forgType="unknown",forgID="unknown",forgUrl="unknown"):
        if userID == "unknown":
            self.NODE_ID = "unknown"
        else:
            self.NODE_ID = "ESRI_USER_" + userID
        self.foundOn = {"orgType": forgType,"orgCode":forgID, "orgUrl":forgUrl}
        self.orgType = orgType
        self.orgID = orgID
        self.orgUrl = orgUrl
        self.orgNODE_ID = orgType + "_" + orgID

        # uniqe for users
        self.username = username
        self.fullName = fullName
        self.email = email.lower()
        self.userRole = userRole
        self.userLevel = userLevel
        self.userID = userID
        if self.orgUrl != "unknown":
            self.homepage = self.orgUrl + "/home/user.html?user=" + self.username
        else:
            self.homepage = ""

        # Time dependent
        self.str_time = "'%Y-%m-%dT%H:%M:%S'"
        try:
            self.created = datetime.datetime.fromtimestamp(created_epoch / 1000)
            self.created = datetime.datetime.strftime(self.created,self.str_time)
        except:
            self.created = "1970-01-01T01:00:00"


        try:
            self.lastLogin = datetime.datetime.fromtimestamp(lastLogin_epoch/1000)
            self.lastLogin = datetime.datetime.strftime(self.lastLogin, self.str_time)
        except:
            self.lastLogin = "1970-01-01T01:00:00"


    def __repr__(self):
        return [self.username, self.userRole, self.userLevel, self.orgUrl]

    def __str__(self):
        ret = "Nodetype: User \n Lable: {8}\n properties: \n\tusername: {0}\n\trole:\t{1}\n\tlevel:\t{2}\n\tcreated:\t{3}\n\tLast login:\t{4}\n RELATIONS:\n\t-[OWNER]-> {5} \n\t-[ON]-> {6}\n\t-[ACCESSES]-> {7}".format(
            self.username, self.userRole, self.userLevel, self.created, self.lastLogin, self.email, self.tech, self.accessDomain, self.userId
        )
        return ret

    def csv_list(self):
        return [self.NODE_ID, self.username, self.userRole, self.userLevel, self.created, self.lastLogin, self.homepage]

    def csv_list_temp(self):
        return [self.NODE_ID, self.username, self.orgNODE_ID]

class Group:
    def __init__(self,groupID="unknown",title="unknown",homepage="unknown",access="private", groupMembers="", tags="",orgType="unknown",orgID="unknown",orgUrl="unknown", forgType="unknown",forgID="unknown",forgUrl="unknown"):
        self.groupID = groupID
        if groupID == "unknown":
            self.NODE_ID = "unknown"
        else:
            self.NODE_ID = "ESRI_GROUP_" + groupID
        self.foundOn = {"orgType": forgType,"orgCode":forgID, "orgUrl":forgUrl}
        self.orgType = orgType
        self.orgID = orgID
        self.orgUrl = orgUrl
        self.orgNODE_ID = orgType + "_" + orgID
        self.access = access

        # uniqe for groups
        self.title = title
        self.homepage = homepage
        self.groupMembers = groupMembers
        self.tags = tags

        self.header = ["NODE_ID", "group_id","title","homepage","orgNodeId"]

    def csv_list(self):
        return [self.NODE_ID,self.groupID,self.title,self.homepage,self.orgNODE_ID,]

    def csv_Item_list(self):
        return [self.NODE_ID, self.groupID, self.orgNODE_ID]

class AccessDomain:
    def __init__(self, url, name, code="",type=""):
        self.url = url
        if type == "PUBLIC_DOMAIN":
            self.url = "PUBLIC_DOMAIN"
        self.name = name
        self.code = code
        self.type = type

        self.NODE_ID = self.type + "_" + self.code
        if type == "PUBLIC_DOMAIN":
            self.url = "PUBLIC_DOMAIN"
            self.NODE_ID = "PUBLIC_DOMAIN"

    def csv_list(self):
        return [self.NODE_ID,self.url,self.name,self.code, self.type]

class EmailEntity:
    def __init__(self, email, fullName=""):
        self.NODE_ID = str(email).lower()
        self.email = str(email).lower()                      #Lable
        self.user = self.email.split("@")[0].upper()
        self.domain = self.email.split("@")[-1].split(".")[0]
        self.topLevelDomain = self.email.split(".")[-1]

        self.fullName = [fullName] # from user

    def __str__(self):
        return str(self.user) + ": " + self.email + "\n\t names: " + str(self.fullName)

    def csv_list(self):
        return [self.NODE_ID,self.email,self.user,self.domain,self.topLevelDomain,str(self.fullName)]

class CowiEntity:
    def __init__(self, initials):
        self.NODE_ID = initials.upper()
        self.cowiInitials = initials.upper()
        self.cowiStandardEmail = initials.lower() + "@cowi.com"

    def __str__(self):
        return self.cowiInitials

    def csv_list(self):
        return [self.NODE_ID,self.cowiInitials,self.cowiStandardEmail]

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

class Tag:
    def __init__(self, tag):
        self.NODE_ID = "TAG_" + tag
        self.tag = tag

    def csv_list(self):
        return [self.NODE_ID, self.tag]

# misc functions
def findUserID(username, orgNODE_ID, Users, FalseAsUnknown=False):
    ID = False
    for row in Users.keys():
        if Users[row].orgNODE_ID == orgNODE_ID:
            if username == Users[row].username:
                ID = Users[row].userID
    # if FalseAsUnknown and id is False:
    #     print("unknown user.")
    #     return "Unknown"
    return ID

def resolveUnknownUsersFromGroups(Users,Groups,gis,org):
    all_unresolved_users = {}
    new_resolved = {}
    for group in Groups:
        members = []
        # gathering all users onto one list
        if Groups[group].groupMembers["admins"] is not None:
            members += Groups[group].groupMembers["admins"]
        if Groups[group].groupMembers["users"] is not None:
            members += Groups[group].groupMembers["users"]

        # Finding all members, regardless of type
        for user in members:
            finduser = findUserID(user, Groups[group].orgNODE_ID, Users)
            if finduser  == False:
                if user + "_" + Groups[group].NODE_ID not in all_unresolved_users.keys():
                    all_unresolved_users[user + "_" + Groups[group].NODE_ID] = [user, org_code]


    # for each unresolved user; see if there is data, then add "outside org"
    for user in all_unresolved_users.keys():
        try:
            gis_user = arcgis.gis.User(gis, all_unresolved_users[user][0])
            if "ESRI_USER_" + gis_user.id in Users.keys():
                if Users["ESRI_USER_"+gis_user.id].userRole == "OUTSIDE_ORG":
                    temp_user = User(username=gis_user.username,
                                     fullName=gis_user.fullName,
                                     userRole="OUTSIDE_ORG",
                                     userID=gis_user.id,
                                     forgType=org[5],
                                     forgUrl=gis.url,
                                     forgID=org[1])
                    new_resolved[temp_user.NODE_ID] = temp_user

                else:
                    pass
                    #print("\t\t was in another org. all data should be here.")
            else:
                #print("\t\t appended new unknown user. ")
                temp_user = User(username=gis_user.username,
                                 fullName=gis_user.fullName,
                                 userRole="OUTSIDE_ORG",
                                 userID=gis_user.id,
                                 forgType=org[5],
                                 forgUrl=gis.url,
                                 forgID=org[1])
                new_resolved[temp_user.NODE_ID] = temp_user

        except:
            print(all_unresolved_users[user])
            print("COULDENT GET DATA for USER")

    return new_resolved

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
                elif type(element) == "str":
                    w_list.append(element.replace("\n", " <br/> ").replace(",","|"))
                else:
                    try:
                        w_list.append(element.replace("\n"," <br/> ").replace(",","|"))
                    except:
                        w_list.append(element)
            writer.writerow(w_list)

# Gather functions
def getListOfUsers(gis,org, query=None):
    CONNECT = arcgis.gis.UserManager(gis)
    allUsers = CONNECT.search(query=None, max_users=10000)
    found_users = {}

    for gis_user in allUsers:
        temp_user = User(username=gis_user.username,
                    email=gis_user.email,
                    fullName=gis_user.fullName,
                    userRole=gis_user.role,
                    userLevel=gis_user.level,
                    userID=gis_user.id,
                    created_epoch=gis_user.created,
                    lastLogin_epoch=gis_user.lastLogin,
                    forgType=org[5],
                    forgUrl=gis.url,
                    forgID=org[1],
                    orgType=org[5],
                    orgUrl=org[2],
                    orgID=org[1])
        found_users[temp_user.NODE_ID] = temp_user


    return found_users

def getListOfGroups(gis, org, query=""):
    groups = arcgis.gis.GroupManager(gis)
    allGroups = groups.search(query=query, max_groups=1000)#, outside_org=True)

    output_groups = {}
    for group in allGroups:
        group_data = arcgis.gis.Group(gis, group.id)
        temp_group = Group(groupID=group.id,
                           title=group.title,
                           homepage=group_data.homepage,
                           groupMembers=group_data.get_members(),
                           tags=group.tags,
                           access=group.access,
                           forgType=org[5],
                           forgUrl=gis.url,
                           forgID=org[1],
                           orgType=org[5],
                           orgUrl=org[2],
                           orgID=org[1]
                           )
        #print(temp_group.csv_list())
        output_groups[temp_group.NODE_ID] = temp_group


    return output_groups

# Generate Nodes functions
def generateEmailEntities(users):
    email_entities = {}

    for user in users:
        if users[user].email is not None and users[user].email != "":
            if users[user].email not in email_entities.keys():
                email_entities[users[user].email] = EmailEntity(users[user].email, users[user].fullName)

            elif users[user].fullName not in email_entities[users[user].email].fullName:
                email_entities[users[user].email].fullName.append(users[user].fullName)
            else:
                pass
    return email_entities

def generateCowiEntities(emailEntitis):
    entities = {}

    for email in emailEntitis.keys():
        if emailEntitis[email].user not in entities.keys():
            if emailEntitis[email].domain.upper() == "COWI":
                entities[emailEntitis[email].user] = CowiEntity(emailEntitis[email].user)

    return entities

def generateTagsAndTagRelations(Groups={},tagRelations={}):
    entities_Group_Tag = {}
    entities_Tag = {}
    i = 0

    for group in Groups.keys():
        for tag in Groups[group].tags:
            i += 1
            entities_Group_Tag[str(i)] = Relationship(From=Groups[group].NODE_ID, To="TAG_" + tag)
            if tag not in entities_Tag.keys():
                entities_Tag[tag] = Tag(tag=tag)
    return entities_Group_Tag, entities_Tag

# Generate Relations functions
def generateUserAccessDomainRelation(Users):
    entities = {}
    i = 0

    for user in Users:
        entities[Users[user].NODE_ID] = Relationship(From=Users[user].NODE_ID, To=Users[user].orgNODE_ID)
        i += 1
    return entities

def generateUserEmailRelation(Users):
    entities = {}
    i = 0

    for user in Users:
        if Users[user].email != "":
            entities[i] = Relationship(Users[user].NODE_ID,Users[user].email)
            i += 1
    return entities

def generateGroupUserRelation(Groups,Users, type="owner", accessdomain={}):
    entities = {}
    i = 0
    #print ("TYPE: ", type)
    for group in Groups:
        # always one owner.
        usersOfType = Groups[group].groupMembers[type]
        if isinstance(usersOfType, str):
            # First the same accessDomain
            user_ID = findUserID(usersOfType, Groups[group].orgNODE_ID, Users)
            if user_ID != False:
                i += 1
                entities[i] = Relationship(Groups[group].NODE_ID, user_ID)

            # checks other accessDomains
            else:
                for row in accessdomain.keys():
                    #Only in same type. EI. Enterprice users cant be on ArcGIS Online
                    if accessdomain[row].type == Groups[group].orgType:
                        user_ID = findUserID(usersOfType,accessdomain[row].NODE_ID,Users)
                        if user_ID != False:
                            if Groups[group].orgNODE_ID == accessdomain[row].NODE_ID:
                                i += 1
                                entities[i] = Relationship(Groups[group].NODE_ID, user_ID)
                                break
                            else:
                                i += 1
                                entities[i] = Relationship(Groups[group].NODE_ID, user_ID)

        elif isinstance(usersOfType, list):
            for user in usersOfType:
                # First the same accessDomain
                user_ID = findUserID(user,Groups[group].orgNODE_ID,Users)
                if user_ID != False:
                    i += 1
                    entities[i] = Relationship(Groups[group].NODE_ID, user_ID)

                # checks other accessDomains
                else:
                    for row in accessdomain.keys():
                        if accessdomain[row].type == Groups[group].orgType:
                            user_ID = findUserID(user, accessdomain[row].code, Users)
                            if user_ID != False:
                                if Groups[group].orgNODE_ID == accessdomain[row].NODE_ID:
                                    i += 1
                                    entities[i] = Relationship(Groups[group].NODE_ID, user_ID)
                                    break
                                else:
                                    i += 1
                                    entities[i] = Relationship(Groups[group].NODE_ID, user_ID)
        i += 1
    return entities

def generateGroupAccessDomainRelation_ON(Groups,AccessDomains):
    entities = {}
    i = 0

    for group in Groups:
        entities[Groups[group].orgID + str(i)] = Relationship(Groups[group].NODE_ID, Groups[group].orgNODE_ID)
        i += 1
    return entities

def generateGroupAccessDomainRelation_SHARED_WITH(Groups,AccessDomains):
    entities = {}
    i = 0

    for group in Groups:
        if Groups[group].access == "org":
            entities[Groups[group].orgID + str(i)] = Relationship(Groups[group].NODE_ID, Groups[group].orgNODE_ID)
            i += 1
        elif Groups[group].access == "public":
            entities[Groups[group].orgID + str(i) + "org"] = Relationship(Groups[group].NODE_ID, Groups[group].orgNODE_ID)
            entities[Groups[group].orgID + str(i) + "public"] = Relationship(Groups[group].NODE_ID, "PUBLIC_DOMAIN")
            i += 1
    return entities

def generateEmailCowiRelation(EmailEntitis):
    entities = {}
    i = 0

    for email in EmailEntitis:
        if EmailEntitis[email].domain.lower() == "cowi":
            entities[i] = Relationship(EmailEntitis[email].NODE_ID,EmailEntitis[email].user)
            i += 1
    return entities


# data
Users = {}
Groups = {}
AccessDomains = {}
EmailEntities = {}
CowiEntities = {}
Tags = {}

User_Email = {}
User_AccessDomain = {}
Email_COWI = {}
Group_Tag = {}
Group_User_OWNER = {}
Group_User_MEMBER = {}
Group_User_MANAGER = {}
Group_AccessDomain_ON = {}
Group_AccessDomain_SHARED_WITH = {}




AccessDomains["PUBLIC_DOMAIN"] = AccessDomain(url="",name="public domain",code="", type="PUBLIC_DOMAIN")

print("Starting to gather data")
for org in Neo4j.organizations:
    print("\t Working width ", org[2])

    # Gather all users
    org_name = org[0]
    org_code = org[1]
    org_type = org[5]
    gis = GIS(org[2], username=org[3], password=org[4])
    print("\t\t Connected. ")


    # Add AccessDomain Nodes
    AccessDomains[org_code] = AccessDomain(url=org[2],name=org_name,code=org_code, type=org_type)

    # Get data from API
    print("\t\t Getting users...")
    temp_users = getListOfUsers(gis,org, query=None)  # some error at portals. has to be None, not "".
    Users.update(temp_users)
    print("\t\t\t", len(temp_users), " users gathered.")


    # Gather all groups
    print("\t\t Getting Groups...")
    temp_groups = getListOfGroups(gis, org, query=Neo4j.query)
    Groups.update(temp_groups)
    print("\t\t\t", len(temp_groups), " groups gathered.")


    # Resolve unknown users
    print("\t\t Resolving Unknown users...")
    temp_unknown_users = resolveUnknownUsersFromGroups(Users, temp_groups, gis=gis, org=org)
    Users.update(temp_unknown_users)
    print("\t\t\t", len(temp_unknown_users), " outside users gathered.")


print("Total found on all searched organizations:")
print("\t", len(Groups), " Groups gathered total.")
print("\t", len(Users), " Users gathered total.")


# Generating email entities
print("Generating inferred nodes")
EmailEntities = generateEmailEntities(Users)
print("\t", len(EmailEntities), " Email entities found.")

CowiEntities = generateCowiEntities(EmailEntities)
print("\t", len(CowiEntities), " COWI entities found.")

Group_Tag, Tags = generateTagsAndTagRelations(Groups=Groups)
print("\t", len(Tags), " Tags found.")

print("Resolving relations")
Group_User_OWNER = generateGroupUserRelation(Groups=Groups, Users=Users, type="owner", accessdomain=AccessDomains)
Group_User_MEMBER = generateGroupUserRelation(Groups=Groups, Users=Users, type="users", accessdomain=AccessDomains)
Group_User_MANAGER = generateGroupUserRelation(Groups=Groups, Users=Users, type="admins", accessdomain=AccessDomains)
Group_AccessDomain_ON = generateGroupAccessDomainRelation_ON(Groups,AccessDomains)
Group_AccessDomain_SHARED_WITH = generateGroupAccessDomainRelation_SHARED_WITH(Groups,AccessDomains)
User_Email = generateUserEmailRelation(Users)
Email_COWI = generateEmailCowiRelation(EmailEntities)
User_AccessDomain = generateUserAccessDomainRelation(Users)

print("\t", len(User_Email), " User - Email OWNER relations found.")
print("\t", len(User_AccessDomain), " User - AccessDomain ON relations found.")
print("\t", len(Group_Tag), " Group - Tag HAS relations found.")
print("\t", len(Group_User_OWNER), " Group - User OWNER relations found.")
print("\t", len(Group_User_MEMBER), " Group - User MEMBER relations found.")
print("\t", len(Group_User_MANAGER), " Group - User MANAGER relations found.")
print("\t", len(Group_AccessDomain_ON), " Group - AccessDomain ON relations found.")
print("\t", len(Group_AccessDomain_SHARED_WITH), " Group - AccessDomain SHARED_WITH relations found.")
print("\t", len(Email_COWI), " Email - COWI OWNER relations found.")




print("Printing CSV files for use in Neo4j")
list_of_csv_exports = [
    # Nodes
    # Dataset       Name of CSV      Header
    [EmailEntities, "ADLR_NODE_EmailEntity", ["NODE_ID", "email", "user","domain", "topDomain","names"],["str","str","str","str","str","list"]],
    [CowiEntities, "ADLR_NODE_CowiEntity", ["NODE_ID", "username", "standardCowiEmail"],["str","str","str"]],
    [Users,"ADLR_NODE_User",["NODE_ID", "username", "role", "level", "created", "lastLogin","homepage"],["str","str","str","str","Datetime","Datetime","str"]],
    [AccessDomains,"ADLR_NODE_AccessDomain", ["NODE_ID","url","name","code","type"],["str","str","str","str","str"]],
    [Groups,"ADLR_NODE_Group", ["NODE_ID", "id", "title","homepage"],["str","str","str","str"]],
    [Tags,"ADLR_NODE_Tag",["NODE_ID","tag"],["str","str"]],


    # Relations
    [User_Email,"ADLR_REL_User_EmailEntity_OWNER",["From","To"],["str","str"]],
    [User_AccessDomain,"ADLR_REL_User_AccessDomain_ON",["From","To"],["str","str"]],

    [Email_COWI,"ADLR_REL_EmailEntity_CowiEntity_OWNER",["From","To"],["str","str"]],

    [Group_User_OWNER,"ADLR_REL_Group_User_OWNER",["From","To"],["str","str"]],
    [Group_User_MANAGER,"ADLR_REL_Group_User_MANAGER",["From","To"],["str","str"]],
    [Group_User_MEMBER,"ADLR_REL_Group_User_MEMBER",["From","To"],["str","str"]],
    [Group_AccessDomain_ON,"ADLR_REL_Group_AccessDomain_ON",["From","To"],["str","str"]],
    [Group_AccessDomain_SHARED_WITH,"ADLR_REL_Group_AccessDomain_SHARED_WITH",["From","To"],["str","str"]],
    [Group_Tag,"ADLR_REL_Group_Tag_HAS_TAG",["From","To"],["str","str"]],

]

for csvfile in list_of_csv_exports:
    printToCSV([csvfile[0][x].csv_list() for x in csvfile[0].keys()], csvfile[1], Neo4j.export_folder, csvfile[2])
    print("\t", csvfile[1], " created.")




print("Printing CSV files for use in Items script")
printToCSV([Users[x].csv_list_temp() for x in Users.keys()], "Users", Neo4j.temp_folder, ["NODE_ID", "username", "orgNODE_ID"])
print("\t", "temp_users", " created.")
