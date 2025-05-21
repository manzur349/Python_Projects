import re, time, sys
from geopy.geocoders import Nominatim

def get_number_of_shelters(myDoc):
    outVal = 0
    for i in range(len(myDoc)):
        if i == 0:
            outVal += 1
        else:
            currentLine = myDoc[i].strip()
            if len(currentLine) == 0:
                if i + 1 < len(myDoc):
                    outVal +=1
    return outVal

def get_shelter_details(myData, myShelter):
    for i in range(len(myData)):
        if i == 0:
            myShelter["name"] = myData[i]
        elif myData[i][0] == "(" and "-" in myData[i]:
            myTokens = myData[i].split("-")
            if re.match('^\d{4}', myTokens[len(myTokens) - 1]):
                myShelter["phone"] = myData[i]
        else:
            myTokens = myData[i].split(" ")
            if re.match('^\d{5}', myTokens[len(myTokens) - 1]):
                myShelter["address"] = myData[i]
    return myShelter

def get_shelters(myDoc):
    outVal = []
    for i in range(len(myDoc)):
        shelter = {}
        shelter["name"] = ""
        shelter["phone"] = ""
        shelter["address"] = ""
        shelter["latitude"] = ""
        shelter["longitude"] = ""
        dataBlock = []
        currLine = i
        if currLine == 0:
            while len(myDoc[currLine]) != 0 and currLine < len(myDoc):
                dataBlock.append(myDoc[currLine])
                currLine += 1
            shelter = get_shelter_details(dataBlock, shelter)
            outVal.append(shelter)
        elif len(myDoc[currLine]) == 0 and currLine + 1 < len(myDoc):
            currLine += 1
            while currLine < len(myDoc) and len(myDoc[currLine]) != 0:
                dataBlock.append(myDoc[currLine])
                currLine += 1
            shelter = get_shelter_details(dataBlock, shelter)
            outVal.append(shelter)
    return outVal

def load_data_file(fileName):
    outVal = []
    myFizzle = open(fileName, "r")
    myLine = myFizzle.readline()
    while myLine:
        outVal.append(myLine.strip())
        myLine = myFizzle.readline()
    myFizzle.close()
    return outVal

def main():
    fileArg = sys.argv[1]
    myFile = load_data_file(fileArg)
    myoutput = open("my_results.txt", "w")
    myShelters = get_shelters(myFile)
    for shelter in myShelters:
        print(shelter["name"])
        print(shelter["address"])
        myoutput.write(shelter["name"]+"\n")
        myoutput.write(shelter["address"]+"\n")
        myoutput.write(shelter["phone"]+"\n")
        geolocator = Nominatim(user_agent="specify_your_app_name_here")
        location = geolocator.geocode(shelter["address"], timeout=10)
        if location:
            print((location.latitude, location.longitude))
            myoutput.write("latitude: " + str(location.latitude)+"\n")
            myoutput.write("longitude: " + str(location.longitude)+"\n")
        else:
            print("latitude and longitude not found.")
            myoutput.write("latitude: not found\n")
            myoutput.write("latitude: not found\n")
        print("------------------")
        myoutput.write("\n")
        time.sleep(3)
    #numberOfShelters = get_number_of_shelters(myFile)
    #print("There are " + str(numberOfShelters) + " shelters in the file.")

if __name__ == "__main__":
    main()
