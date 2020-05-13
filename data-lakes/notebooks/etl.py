
codes = dict()

"""with open('t.txt') as f:
    for line in f.readlines():
        s, b = line.split(' = ')
        b = b.replace("'", "").strip()
        print(s,b)
        codes[int(s)] = b
        #"".join(.split()).strip("'")

print(codes)
"""



codes = dict()

a, ba , ca = list(), list(), list()
with open('airp.txt') as f:
    for line in f.readlines():
        #print(line.split("="))
        s, b = line.split('=')
        s = s.strip().replace("'", "")
        b = " ".join(b.strip().replace("'", "").split()).split(',')
        #print(b)
        #print(b[1])
        a.append(s)
        if len(b) == 2:
            b1 = b[0]
            c = b[1].strip() 
        else:
            c = s 
            b1 = s
        ba.append(b1)
        ca.append(c)
        codes[s] = b
        #print(s.strip(), b)
        #b = b.replace("'", "").strip()
        #print(s,b)
        #codes[int(s)] = b
        #"".join(.split()).strip("'")

print(len(a), len(ba), len(ca))
print(ca)
"""
#print(codes)


def gen(col:list, dtype:list):
    check_data = dict()
    check_data["query"] = f"{col[0]} is null or {col[1]} is null"
    check_data["dtype1"] = f"{dtype[0]}"
    check_data["dtype2"] = f"{dtype[1]}"
    check_data["name1"] = f"{col[0]}"
    check_data["name2"] = f"{col[1]}"
    return check_data
col = ["airport", "id"]
dtype = ["string", "int"]

ch = gen(col, dtype)

print(ch["query"])"""