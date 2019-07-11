// var c = db.lineorder.findOne({city_pk:20});
// address=16022 contains city=20, so select this address and get only city geo.
var c = db.lineorder.findOne({c_address_pk : 16022}, {city_geo:1})

var result = db.lineorder.aggregate([
   {
      $match: {
          c_address_geo: {
             $geoWithin: {
                $geometry: c.city_geo
             }
          }
      }
   },
   { $project : { "c_address_pk": 1 } }, 
   { $match : { 'c_address_pk': {$exists:true } }},     
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "c_address_pk",
           foreignField: "c_address_fk",
           as: "lineorder_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$lineorder_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
   { $project : { "lineorder_join": 1 } }, 	
   {
       $match: 
       {
            'lineorder_join.p_category': "MFGR#25"
       }
   },    
   {
       $group:
       {
            _id : { year: "$lineorder_join.d_year" ,  p_brand1: "$lineorder_join.p_brand1"},
           total_revenue: { $sum: "$lineorder_join.lo_revenue" }
       }
   },
   { 
        $sort:
        { 
           '_id.year' : 1,
            '_id.p_brand1': 1
        }
   }
   
]).toArray()

print("results = " + result.length)
printjson(result)


