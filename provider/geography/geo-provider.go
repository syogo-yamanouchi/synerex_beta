package main

// Geographic data provider

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/go-spatial/proj"
	"github.com/golang/protobuf/proto"

	geojson "github.com/paulmach/go.geojson"

	geo "github.com/synerex/proto_geography"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	geoJsonFile     = flag.String("geojson", "", "GeoJson file")
	topLabel        = flag.String("topLabel", "", "Top Label Text")
	topStyle        = flag.String("topStyle", "", "Top Label Style")
	label           = flag.String("label", "", "Label of data")
	lines           = flag.String("lines", "", "geojson for lines")
	viewState       = flag.String("viewState", "", "set ViewState as lat,lon,zoom,pitch")
	bearing         = flag.String("bearing", "", "set bearing")
	pitch           = flag.String("pitch", "", "set pitch")
	duration        = flag.Float64("duration", 0.0, "animation duration(sec) for Pitch,Bearing,ViewState")
	clearMoves      = flag.String("clearMoves", "", "moves data clear message")
	harmovis        = flag.String("harmovis", "", "harmovis json config")
	webmercator     = flag.Bool("webmercator", false, "if set, lat, lon projection is in webmercator")
	idnum           = flag.Int("id", 1, "ID of data")
	sxServerAddress string
)

func convertGeoJsonMercator(bytes []byte) []byte {
	jsonData, _ := geojson.UnmarshalFeatureCollection(bytes)
	//	type := jsonData.Type
	fclen := len(jsonData.Features)
	fmt.Printf("convertGeoJsonMercator! %d\n", fclen)

	for i := 0; i < fclen; i++ {
		geom := jsonData.Features[i].Geometry
		//		fmt.Printf("%#v", geom)
		if geom.IsMultiLineString() {
			log.Printf("MulitiLine %d: %#v", i, geom)
			for k := 0; k < len(geom.MultiLineString); k++ {
				coord := geom.MultiLineString[k]
				ll := len(coord)
				for j := 0; j < ll; j++ {
					latlon := webmercator2latlon(coord[j][0], coord[j][1])
					geom.MultiLineString[k][j][0] = latlon[0]
					geom.MultiLineString[k][j][1] = latlon[1]
				}
			}
		}
		if geom.IsMultiPolygon() {
			coord := geom.MultiPolygon[0][0]
			ll := len(coord)
			log.Printf("MulitPolygon %d", ll)
			for j := 0; j < ll; j++ {
				latlon := webmercator2latlon(coord[j][0], coord[j][1])
				//				fmt.Printf("%f,%f -> #%v \n", coord[j][0],coord[j][1], latlon)
				geom.MultiPolygon[0][0][j][0] = latlon[0]
				geom.MultiPolygon[0][0][j][1] = latlon[1]
			}

		}
		if geom.IsPolygon() {
			coord := geom.Polygon[0]
			ll := len(coord)
			log.Printf("Polygon Len %d", ll)
			for j := 0; j < ll; j++ {
				//				log.Printf("MulitiPolygon %d: %#v", i, geom)
				latlon := webmercator2latlon(coord[j][0], coord[j][1])
				//				fmt.Printf("%f,%f -> #%v \n", coord[j][0],coord[j][1], latlon)
				geom.Polygon[0][j][0] = latlon[0]
				geom.Polygon[0][j][1] = latlon[1]
			}

		}

	}

	bt, _ := jsonData.MarshalJSON()

	return bt
}

func sendViewState(client *sxutil.SXServiceClient, str string, duration float64) {
	lat := 34.8592285
	lon := 136.8163486
	zoom := -1.0  // no change
	pitch := -1.0 // no change

	fmt.Sscanf(str, "%f,%f,%f,%f", &lat, &lon, &zoom, &pitch)
	vsd := geo.ViewState{
		Lat:      lat,
		Lon:      lon,
		Zoom:     zoom,
		Pitch:    pitch,
		Duration: duration,
	}
	out, _ := proto.Marshal(&vsd) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "ViewState",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func sendGeoJsonFile(client *sxutil.SXServiceClient, id int, label string, fname string) {
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Print("Can't read file:", err)
		panic("load json")
	}

	if *webmercator {
		bytes = convertGeoJsonMercator(bytes)
	}

	geodata := geo.Geo{
		Type:  "geojson",
		Id:    int32(id),
		Label: label,
		Data:  bytes,
	}

	out, _ := proto.Marshal(&geodata) // TODO: handle error

	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "GeoJson",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func loadGeoJSON(fname string) *geojson.FeatureCollection {
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Print("Can't read file:", err)
		panic("load json")
	}

	fc, _ := geojson.UnmarshalFeatureCollection(bytes)

	return fc
}

func webmercator2latlon(x float64, y float64) []float64 {
	var xy = []float64{x, y}
	latlon, _ := proj.Inverse(proj.WebMercator, xy)
	return latlon
}

func sendLines(client *sxutil.SXServiceClient, id int, label string, fname string) {

	jsonData := loadGeoJSON(fname)

	fcs := jsonData.Features
	//	type := jsonData.Type
	fclen := len(fcs)
	lines := make([]*geo.Line, 0, fclen)
	log.Printf("Fetures: %d", fclen)
	for i := 0; i < fclen; i++ {
		geom := fcs[i].Geometry
		//		log.Printf("MulitiLine %d: %v", i, geom.)
		if geom.IsMultiLineString() {
			log.Printf("MultilineString Len %d", len(geom.MultiLineString))
			for k := 0; k < len(geom.MultiLineString); k++ {
				coord := geom.MultiLineString[k]
				ll := len(coord)
				for j := 0; j < ll-1; j++ {

					if *webmercator {

						lines = append(lines, &geo.Line{
							From: webmercator2latlon(coord[j][0], coord[j][1]),
							To:   webmercator2latlon(coord[j+1][0], coord[j+1][1]),
						})
					} else {
						lines = append(lines, &geo.Line{
							From: []float64{coord[j][0], coord[j][1]},
							To:   []float64{coord[j+1][0], coord[j+1][1]},
						})
					}
				}
			}
		}
		if geom.IsMultiPolygon() {
			//			log.Printf("MultiPolygon %d: %v", i, geom)
			coord := geom.MultiPolygon[0][0]
			ll := len(coord)
			for j := 0; j < ll-1; j++ {
				if *webmercator {
					lines = append(lines, &geo.Line{
						From: webmercator2latlon(coord[j][0], coord[j][1]),
						To:   webmercator2latlon(coord[j+1][0], coord[j+1][1]),
					})
				} else {
					lines = append(lines, &geo.Line{
						From: []float64{coord[j][0], coord[j][1]},
						To:   []float64{coord[j+1][0], coord[j+1][1]},
					})
				}
			}
			if *webmercator {

				lines = append(lines, &geo.Line{
					From: webmercator2latlon(coord[ll-1][0], coord[ll-1][1]),
					To:   webmercator2latlon(coord[0][0], coord[0][1]),
				})
			} else {

				lines = append(lines, &geo.Line{
					From: []float64{coord[ll-1][0], coord[ll-1][1]},
					To:   []float64{coord[0][0], coord[0][1]},
				})
			}
		}

	}

	geodata := geo.Lines{
		Lines: lines,
		Width: 1,
	}

	out, _ := proto.Marshal(&geodata) // TODO: handle error

	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "Lines",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func sendBearing(client *sxutil.SXServiceClient, str string, duration float64) {
	bearing0 := 0.0

	fmt.Sscanf(str, "%f", &bearing0)
	vsd := geo.Bearing{
		Bearing:  bearing0,
		Duration: duration,
	}
	out, _ := proto.Marshal(&vsd) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "Bearing",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}
func sendPitch(client *sxutil.SXServiceClient, str string, duration float64) {
	pitch0 := 0.0

	fmt.Sscanf(str, "%f", &pitch0)
	vsd := geo.Pitch{
		Pitch:    pitch0,
		Duration: duration,
	}
	out, _ := proto.Marshal(&vsd) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "Pitch",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}
func sendClearMoves(client *sxutil.SXServiceClient, str string) {

	vsd := geo.ClearMoves{
		Message: str,
	}
	out, _ := proto.Marshal(&vsd) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "ClearMoves",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func sendTopLabel(client *sxutil.SXServiceClient, label string, style string) {
	msg := geo.TopTextLabel{
		Label: label,
		Style: style,
	}

	out, _ := proto.Marshal(&msg) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "TopTextLabel",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func sendHarmoVIS(client *sxutil.SXServiceClient, conf string) {
	msg := geo.HarmoVIS{
		ConfJson: conf,
	}

	out, _ := proto.Marshal(&msg) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "HarmoVIS",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func main() {
	log.Printf("Geo-Provider(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.GEOGRAPHIC_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "GeoService", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Client:GeoService}")
	sclient := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON)

	if *harmovis != "" {
		sendHarmoVIS(sclient, *harmovis)
	}

	if *geoJsonFile != "" {
		sendGeoJsonFile(sclient, *idnum, *label, *geoJsonFile)
	}
	if *lines != "" {
		sendLines(sclient, *idnum, *label, *lines)
	}
	if *viewState != "" {
		sendViewState(sclient, *viewState, *duration)
	}

	if *bearing != "" {
		sendBearing(sclient, *bearing, *duration)
	}

	if *pitch != "" {
		sendPitch(sclient, *pitch, *duration)
	}

	if *clearMoves != "" {
		sendClearMoves(sclient, *clearMoves)
	}

	if *topLabel != "" {
		sendTopLabel(sclient, *topLabel, *topStyle)
	}

	sxutil.CallDeferFunctions() // cleanup!

}
