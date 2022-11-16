import classes
import sys, getopt



if __name__ == "__main__":
    args = sys.argv[1:]
    opts = getopt.getopt(args, "t:c:d:")[0]
    function_type = ""
    config_path = ""
    folder_path = ""
    for o in opts:
        if o[0] == "-t":
            function_type = o[1]
        elif o[0] == "-c":
            config_path = o[1]
        elif o[0] == "-d":
            folder_path = o[1]


    if not (function_type and config_path and folder_path):
        print("Invalid paremeters...")
    if function_type == "create":
        classes.snow.create(folder_path)
    elif function_type == "insert":
        classes.snow.insert(folder_path)
    elif function_type == "fetch":
        classes.help.output()
        classes.text.creat_text_file()
       
    





