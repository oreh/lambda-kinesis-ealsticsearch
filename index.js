var elasticsearch = require('elasticsearch');
var step = require('step')

var client = new elasticsearch.Client({
  host: <ip>+':9200'
});

var es_index = ""
var es_type = "iot_data"

function dateToYMD(date) {
    var d = date.getDate();
    var m = date.getMonth() + 1;
    var y = date.getFullYear();
    return '' + y + '-' + (m<=9 ? '0' + m : m) + '-' + (d <= 9 ? '0' + d : d);
}

exports.handler = function(event, context) {
    // console.log(JSON.stringify(event, null, 2));
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
        var d = JSON.parse(payload);
        var s = d['time'].toString();
        var year = s.substring(0, 4);
        var month = s.substring(4, 6);
        var day = s.substring(6, 8);
        var hour = s.substring(8, 10);
        var minute = s.substring(10, 12);
        var second = s.substring(12, 14);

        var r = {
          'device_id': d['device_id'],
          'time': year+'-'+month+'-'+day+'T'+hour+':'+minute+':'+second+'Z',
          'location': d['lat']+', '+d['lon'],
          'uv': d['data'][0],
          'light': d['data'][1],
          'temp': d['data'][2],
          'humidity': d['data'][3],
          'sound': d['data'][4],
          'dust': d['data'][5],
          'pressure': d['data'][6],
          'altitude': d['data'][7]
        }

        es_index = "iot-" + year + "." + month + "." + day
        console.log('Decoded data');
        console.log(r);

        step(
          function (){
            client.indices.exists({
              index: es_index
            }, this)
          },
          function create_index_if_missing(err, stat){
            if (err){
              console.log(err);
              context.done()
            }

            if (!stat){
              console.log('Create index')  
              client.indices.create({
                  index: es_index
              }, this)
            }
            else{
              return true;
            }
          },
          function create_mapping(err, stat){
            if (err){
              console.log(err);
              context.done()
            }
            if (typeof(stat)=='object'){
              console.log('add mapping');
              var body = {
                'iot_data':{
                    'properties':{
                        'device_id'  : {"type" : "string", "index" : "not_analyzed"},
                        'time'       : {"type" : "date", "format" : "yyyy-MM-dd'T'HH:mm:ss'Z'"},
                        'location'   : {"type" : "geo_point"},
                        'uv'         : {"type" : "float"},
                        'light'      : {"type" : "float"},
                        'temp'       : {"type" : "float"},
                        'humidity'   : {"type" : "float"},
                        'sound'      : {"type" : "float"},
                        'dust'       : {"type" : "float"},
                        'pressure'   : {"type" : "float"},
                        'altitude'   : {"type" : "float"}
                    }
                }
              }
              client.indices.putMapping({index:es_index, type:es_type, body:body}, this)
            }
            else{
              return true;
            }
          },
          function insert_data(err, stat) {
            if (err){
              console.log(err);
              context.done()
            }

            client.create({
                index: es_index,
                type: es_type,
                body: r
            }, this)
          },
          function(err, rep){
            if (err){
              console.log(err);
            }
            /* reply shape
              { 
                _index: 'iot_test_oreh',
                _type: 'iot_data',
                _id: 'AU1cwMPsrnJqSn-3ayB2',
                _version: 1,
                created: true
              }
             */
            // console.log(rep._index+" "+rep._type+" "+rep._id)
            context.done();
          }
        );
    });
};
