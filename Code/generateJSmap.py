import csv

#open JS file we want to create
f = open('1950makeMap.js', 'w')

#take data from 1950corr.txt from analyze and put in the cor list
cor = []
fco = open('Data/1950corr.txt', 'r') 
for c in fco:
	cor.append(float((c)))
fco.close
#create lists for the latitude and longitude
latlon = []
mapData = []
station = wban = 0
l = ""
i = 0
with open('Data/1950years.csv', 'rb') as fin:
	reader = csv.reader(fin, delimiter='\t')
	for line in reader:
		if(station != int(line[0]) or wban != int(line[1])):

            #Get the data from the csv file created from PIG
			station = int(line[0])
			wban = int(line[1])
			key = line[4]
			country = line[5]
			lat = float(line[6]) / 1000.0
			lon = float(line[7]) / 1000.0
			value = cor[i]
            #if positive color red, if neg color blue
			if(value > 0):
                #RED
				color = '#FF0000'
			else:
                #BLUE
				color = '#0000FF'
            #Format the data in the way the JS needs it
			ll = "latlong[\"" + key + "\"] = {\"latitude\":" + str(lat) + ", \"longitude\":" + str(lon) + "};\n"
			md = "{\"code\":\"" + key + "\" , \"name\":\"" + key + "," + country + "\", \"value\":" + str(value) + ", \"color\":\"" + color + "\"},\n"
            #Add that station to the list of lat long and the map data
			latlon.append(ll)
			mapData.append(md)
			i +=1
            #if we get all of the stations that have correlations then stop
			if(len(cor) == i):
				break
fin.close()		
#write all the latitudes and longitudes to the JSfile
f.write("var latlong = {};\n")
for l in latlon:
	f.write(l)

#write all of the station info to the JS file
f.write("\nvar mapData = [\n")
for l in mapData:
	f.write(l)	


#Huge string to put at end of file
bottomOfFile = """
var map;

 // build map
AmCharts.ready(function() {
  	AmCharts.theme = AmCharts.themes.dark;
	map = new AmCharts.AmMap();
  	map.pathToImages = "http://www.http://cs440-jf.s3-website-us-east-1.amazonaws.com/ammap/images/";
	
	map.areasSettings = {
		unlistedAreasColor: "#000000",
		unlistedAreasAlpha: 0.1
	};
	map.imagesSettings.balloonText = "<span style='font-size:14px;'><b>[[title]]</b>: [[value]]</span>";

	var dataProvider = {
		mapVar: AmCharts.maps.worldLow,
		images: []
	}

	// create circle for each country
	for (var i = 0; i < mapData.length; i++) {
		var dataItem = mapData[i];
		var value = dataItem.value;
		// calculate size of a bubble
		var size = 5;
		var id = dataItem.code;

		dataProvider.images.push({
			type: "circle",
			width: size,
			height: size,
			color: dataItem.color,
			longitude: latlong[id].longitude,
			latitude: latlong[id].latitude,
			title: dataItem.name,
			value: value
		});
	}

	map.dataProvider = dataProvider;

	map.write("mapdiv");
});"""
#write the string to the file
f.write(bottomOfFile)
f.close()
