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
            tableName = (line.strip()).lower()
            name = False

        else:
            cols.append((line.strip()).lower())

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
    
    data1 = []

    for r in data:
        li = []
        for item in r:
            li.append(int(item))
        data1.append(li)
    return data1

#-----------------------------------------------------------------------------------------------------------------------------#

def get_col_index(col, tableName):
    i=0
    for c in meta[tableName]:
        if c==col:
            return i
        else:
            i+=1

#-----------------------------------------------------------------------------------------------------------------------------#

def get_col_index_in_cp(col, tables):
    idx = 0

    for tbl in tables:
        if col not in meta[tbl]:
            idx+=len(meta[tbl])
        else:
            idx+=get_col_index(col, tbl)
            break

    return idx

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

    if col=="*":
        col = meta[tableName][0]

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
        #print(wc)
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

def cartisan_product(table1, table2):
    carp = []
    for r1 in table1:
        for r2 in table2:
            li = []
            li.extend(r1)
            li.extend(r2)
            carp.append(li)
    return carp

#-----------------------------------------------------------------------------------------------------------------------------#

def is_col_exist(col, tables):
    for t in tables:
        if col in meta[t]:
            return True
    return False

#-----------------------------------------------------------------------------------------------------------------------------#

def satisfy_cond(num, comp, op):
    if op==">=":
        if num>=comp:
            return True
        else:
            return False

    elif op=="<=":
        if num<=comp:
            return True
        else:
            return False

    elif op=="<":
        if num<comp:
            return True
        else:
            return False

    elif op==">":
        if num>comp:
            return True
        else:
            return False

    elif op=="=":
        if num==comp:
            return True
        else:
            return False

#-----------------------------------------------------------------------------------------------------------------------------#

def filter_where_cond(data, conditions, tables, flag):

    filtered_data = []

    if len(conditions)==1:
        cond_col = conditions[0][0]
        op = conditions[0][2]
        num_constraint = int(conditions[0][1])
        #print(cond_col)
        #print(op)
        #print(num_constraint)
        
        if is_col_exist(cond_col, tables)==False:
            print("\nmysql> Please mention correct coloum in where condition.\n")
            sys.exit()

        idx = get_col_index_in_cp(cond_col, tables)
        
        for row in data:
            if satisfy_cond(int(row[idx]), num_constraint, op):
                filtered_data.append(row)

    else:
        cond_col1 = conditions[0][0]
        op1 = conditions[0][2]
        num_constraint1 = int(conditions[0][1])
        cond_col2 = conditions[1][0]
        op2 = conditions[1][2]
        num_constraint2 = int(conditions[1][1])
        #print(cond_col1)
        #print(op1)
        #print(num_constraint1)
        #print(cond_col2)
        #print(op2)
        #print(num_constraint2)

        if is_col_exist(cond_col1, tables)==False:
            print("\nmysql> Please mention correct coloum in where condition.\n")
            sys.exit()
        if is_col_exist(cond_col2, tables)==False:
            print("\nmysql> Please mention correct coloum in where condition.\n")
            sys.exit()

        idx1 = get_col_index_in_cp(cond_col1, tables)
        idx2 = get_col_index_in_cp(cond_col2, tables)

        if flag:
            for row in data:
                if satisfy_cond(int(row[idx1]), num_constraint1, op1) and satisfy_cond(int(row[idx2]), num_constraint2, op2):
                    filtered_data.append(row)
        else:
            for row in data:
                if satisfy_cond(int(row[idx1]), num_constraint1, op1) or satisfy_cond(int(row[idx2]), num_constraint2, op2):
                    filtered_data.append(row)

        '''print(len(filtered_data))

        for r in filtered_data:
            print(r)'''

    return filtered_data

#-----------------------------------------------------------------------------------------------------------------------------#

def execute_query(tokens, flag_D, flag_O, flag_W):
    
    cols = ""
    tbls = ""
    ws = ""

    flag_AND = False

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
    #print(ws)

    where_cond = []

    if ("AND" in ws) or ("and" in ws):
        flag_AND = True
        if "AND" in ws:
            where_cond = ws.split("AND")
        else:
            where_cond = ws.split("and")

    elif ("OR" in ws) or ("or" in ws):
        if "OR" in ws:
            where_cond = ws.split("OR")
        else:
            where_cond = ws.split("or")
    else:
        where_cond.append(ws)
    
    #print(where_cond)
    if coloums[0] == "*":
        coloums = []
        for t in tables:
            for c in meta[t]:
                coloums.append(c)
    #print(coloums)
    #print(tables)

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

    no_of_tables = len(tables)

    res_data = load_data(tables[0])

    if no_of_tables>1:
        for i in range(1,no_of_tables):
            tbi = load_data(tables[i])
            res_data = cartisan_product(res_data, tbi)

    col_idx_list = []
    for c in coloums:
        col_idx_list.append(get_col_index_in_cp(c, tables))

    #print(col_idx_list)
    
    result = []

    # ----------------------------------------- If where condition is present --------------------------------------#

    if flag_W:
        
        fd1 = []
        conditions = process_operators(where_cond)
        #print(conditions)

        if(len(conditions)==0):
            return

        fd = filter_where_cond(res_data, conditions, tables, flag_AND)
        if(len(fd)==0):
            print("\nmysql> 0 Rows matched with the condition.\n")
            sys.exit()

        for row in fd:
            li = []
            for idx in col_idx_list:
                li.append(row[idx])
            fd1.append(li)

        if flag_D:
            for row in fd1:
                if row not in result:
                    result.append(row)
        else:
            result = fd1 
    
    # ----------------------------------------- If where condition is NOT present --------------------------------------#

    else:
        fd1 = []

        for row in res_data:
            li = []
            for idx in col_idx_list:
                li.append(row[idx])
            fd1.append(li)

        if flag_D:
            for row in fd1:
                if row not in result:
                    result.append(row)
        else:
            result = fd1

    if(len(result)==0):
        print("\nmysql> 0 Rows affected.\n")
        return

    if flag_O:

        tid = 0
        for i in range(0,len(tokens)):
            if tokens[i].lower() == "order by":
                tid = i+1
                break
            
        asc = False
        sort_col = ""

        if ("asc" in tokens[tid]) or ("ASC" in tokens[tid]):
            asc = True
            sort_col = (tokens[tid].replace(" ", ""))[:-3]
        else:
            sort_col = (tokens[tid].replace(" ", ""))[:-4]

        print(sort_col)

        if sort_col not in coloums:
            print("\nmysql> Please enter correct coloumn in order by clause")
            return

        sort_col_idx = 0

        for i in range(0, len(coloums)):
            if coloums[i]==sort_col:
                sort_col_idx = i
                break
        
        print(sort_col_idx)
        print(asc)

        for r in result:
            print(r)

        if asc:
            result.sort(key=lambda x:x[sort_col_idx])
        else:
            result.sort(key=lambda x:x[sort_col_idx], reverse=True)
        #result = sorted(result,key=lambda x: x[sort_col_idx])

        print("\nmysql> Query result :\n")
        for c in coloums:
            print(c.upper(), end = "\t\t ")
        print()
        for row in result:
            for val in row:
                print(val, end = "\t\t")
            print()
        
    else:
        print("\nmysql> Query result :\n")
        for c in coloums:
            print(c.upper(), end = "\t\t ")
        print()
        for row in result:
            for val in row:
                print(val, end = "\t\t")
            print()
    
    print()
    '''
    print("\n Cartesian Product : " + str(len(res_data)))
    for row in res_data:
        print(row)
    
    cursor = {}
    for t in tables:
        data = load_data(t)
        tbdict = {}
        for clms in meta[t]:
            if clms in coloums:
                tbdict[clms] = get_col(clms, t, data)
        cursor[t] = tbdict
    #print("\n Cursor data : ")
    #print(cursor)
    '''
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
    print(tokens)
    
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

    query = take_input().lower()
    parse_query(query)

#-----------------------------------------------------------------------------------------------------------------------------#

main()