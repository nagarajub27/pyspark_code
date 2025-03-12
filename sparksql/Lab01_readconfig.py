import os


def getconfiginfo(configfile):
    
    separator = "="
    props = {}
    with open(configfile) as f:

        for line in f:
            if separator in line:
                name, value = line.split(separator, 1)
                props[name.strip()] = value.strip()
        
    
    return props   

def main():
    configfilepath = os.getcwd().replace("sparksql","config.properties")
    prop = getconfiginfo(configfilepath)
    print(prop.get("jdbcurl"))
    print(prop.get("username"))

main()