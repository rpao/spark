// customer=102 contains region=4
var reg = db.lineorder.findOne({c_custkey:102},{'c_address.nation.region.region_geo':1});
r = reg.c_address[0].nation[0].region[0];


var result = db.lineorder.aggregate([
   {
      $match: {
          'c_address.nation.region.region_geo': {
             $geoIntersects: {
                $geometry: r.region_geo
             }
          }
      }
   },	
   { $project : { "c_custkey": 1 } },    
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "c_custkey",
           foreignField: "lo_custkey",
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
       $lookup:
       {
           from: "lineorder",
           localField: "lineorder_join.lo_partkey",
           foreignField: "p_partkey",
           as: "part_join"
       }
   },   
   {
        $unwind: 
        { 
            path: "$part_join", 
            preserveNullAndEmptyArrays: false
        }  
   },     
   {
       $match: 
       {
            'part_join.p_category': "MFGR#25"
       }
   },  
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "lineorder_join.lo_orderdate",
           foreignField: "d_datekey",
           as: "date_join"
       }
   },   
   {
        $unwind: 
        { 
            path: "$date_join", 
            preserveNullAndEmptyArrays: false
        }  
   },
   {
       $group:
       {
            _id : { year: "$date_join.d_year" ,  p_brand1: "$part_join.p_brand1"},
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

