import sys
import csv
import sqlparse
import re


meta = {}             #-------> for storing metadata
aggregate = ["min", "max", "count", "sum", "avg"]

#-----------------------------------------------------------------------------------------------------------------------------#

def read_meta_data():
    metadata = open("metadata.txt","r")

    name = False
    tableName = ""
    cols = []

    for line in metadata:
        if line.strip() == "<begin_table>":
            name = True

        elif line.strip() == "<end_table>":
            meta[tableName] = cols
            cols = []
            tableName = ""

        elif name == True:
            tableName = line.strip()
            name = False

        else:
            cols.append(line.strip())

#-----------------------------------------------------------------------------------------------------------------------------#

def take_input():
    query = sys.argv[1]
    return query
    #print("\n Entered query: " + query + "\n")

#-----------------------------------------------------------------------------------------------------------------------------#

def load_data(tableName):
    data = []
    fileName = tableName + ".csv" 
    with open(fileName, "r") as file:
        table = csv.reader(file, delimiter=',')
        for row in table:
            data.append(row)
    return data

#-----------------------------------------------------------------------------------------------------------------------------#

def get_col_index(col, tableName):

    i=0
    for c in meta[tableName]:
        if c==col:
            return i
        else:
            i+=1

#-----------------------------------------------------------------------------------------------------------------------------#

def get_col(col, tableName, data):
    idx = get_col_index(col, tableName)
    #print(idx)
    li = []
    for row in data:
        li.append(int(row[idx]))
    return li

#-----------------------------------------------------------------------------------------------------------------------------

def aggregate_query(tokens):

    function = ""
    col = ""
    tableName = tokens[-1]
    if tokens[1][3]!='(':
        function = "count"
        col = tokens[1][6:-1]
    else:
        function = tokens[1][:3]
        col = tokens[1][4:-1]

    #print(function + " | " + col + " | " + tableName)

    if tableName not in meta.keys():
        print("\nmysql> Table " + tableName + " not present in database.\n")
        return

    if col not in meta[tableName]:
        print("\nmysql> Column " + col + " does not exist in " + tableName + ".\n")
        return 

    data = load_data(tableName)
    #print(data)

    idx = get_col_index(col, tableName)
    #print(idx)

    ag = []

    for row in data:
        ag.append(int(row[idx]))

    print(ag)
    
    if(function=="sum"):
        print("\nmysql> " + str(sum(ag)) + "\n")

    elif(function=="min"):
        print("\nmysql> " + str(min(ag)) + "\n")

    elif(function=="max"):
        print("\nmysql> " + str(max(ag)) + "\n")

    elif(function=="count"):
        print("\nmysql> " + str(len(ag)) + "\n")

    elif(function=="avg"):
        print("\nmysql> " + str(float(sum(ag))/float(len(ag))) + "\n")

    else:
        print("\nmysql> Please enter correct Aggregate function")

#-----------------------------------------------------------------------------------------------------------------------------#

def process_operators(where_cond):
    fwc = []
    for wc in where_cond:
        cd = []
        if "<=" in wc:
            cd = wc.split("<=")
            cd.append("<=")
        elif ">=" in wc:
            cd = wc.split(">=")
            cd.append(">=")
        elif ">" in wc:
            cd = wc.split(">")
            cd.append(">")
        elif "<" in wc:
            cd = wc.split("<")
            cd.append("<")
        elif "=" in wc:
            cd = wc.split("=")
            cd.append("=")
        else:
            print("\nmysql> Please mention correct operator in where condition")
            blank = []
            return blank
        fwc.append(cd)        
    return fwc

#-----------------------------------------------------------------------------------------------------------------------------#

def execute_query(tokens, flag_D, flag_O, flag_W):
    
    cols = ""
    tbls = ""
    ws = ""

    flag_AND = False
    flag_OR = False

    if(flag_D):
        cols = tokens[2]
        tbls = tokens[4]

        if(flag_W):
            ws = tokens[5]
    else:
        cols = tokens[1]
        tbls = tokens[3]

        if(flag_W):
            ws = tokens[4]

    cols = cols.replace(" ", "")
    coloums = cols.split(",")
    tbls = tbls.replace(" ", "")
    tables = tbls.split(",")
    ws = (ws.replace(" ",""))[5:]

    where_cond = []

    if ("AND" in ws) or ("and" in ws):
        flag_AND = True
        if "AND" in ws:
            where_cond = ws.split("AND")
        else:
            where_cond = ws.split("and")

    elif ("OR" in ws) or ("or" in ws):
        flag_OR = True
        if "OR" in ws:
            where_cond = ws.split("OR")
        else:
            where_cond = ws.split("or")
    else:
        where_cond = ws
    
    print(coloums)
    print(tables)

    for t in tables:
        if t not in meta.keys():
            print("\nmysql> Please enter correct table names.")
            return

    for c in coloums:
        br = False
        for t in tables:
            if c in meta[t]:
                br = True
                break
        if br==False:
            print("\nmysql> Please enter correct coloumn names.")
            return

    #print(where_cond)
    conditions = process_operators(where_cond)
    if len(conditions)==0:
        return
    print(conditions)
    #print(flag_AND)
    #print(flag_OR)

    cursor = {}
    
    for t in tables:
        data = load_data(t)
        tbdict = {}
        for clms in meta[t]:
            if clms in coloums:
                tbdict[clms] = get_col(clms, t, data)
        cursor[t] = tbdict

    print(cursor)
    
#-----------------------------------------------------------------------------------------------------------------------------#

def parse_query(query):

    #print("\nquery : " + query)

    if query[-1]==";":
        query = query[:-1]

    parsed  = sqlparse.parse(query)
    stmt = parsed[0]
    
    tokens = []

    for tkn in stmt.tokens:
        if str(tkn) != " ":
            tokens.append(str(tkn))
    #print(tokens)
    
    kwrd_distinct = False
    kwrd_order_by = False
    kwrd_where = False

    if re.search("group.*by", query):
        print("\n-> Group by query")
        #group_by()

    elif (tokens[1][:3].lower() in aggregate) or (tokens[1][:5].lower() in aggregate):
        print("\n-> Aggregate query")
        aggregate_query(tokens)

    else: 
        if tokens[1] == "distinct":
            kwrd_distinct = True
            print("\n-> Distinct query")
        
        if "where" in query:
            kwrd_where = True
            print("\n-> Where query")

        if re.search("order.*by", query):
            kwrd_order_by = True
            print("\n-> Order by query")

        execute_query(tokens, kwrd_distinct, kwrd_order_by, kwrd_where)
        
        
#-----------------------------------------------------------------------------------------------------------------------------#

def main():

    read_meta_data()
    #print(meta)

    query = take_input()
    parse_query(query)

    #data = load_data("table1")
    #print(data)

#-----------------------------------------------------------------------------------------------------------------------------#

main()