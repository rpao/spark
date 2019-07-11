//var r = db.lineorder.findOne({region_pk:2});
// customer=201 contains region=2
var r = db.customer.findOne({c_custkey:201}, {region_geo:1});

var result = db.customer.aggregate([
   {
      $match: {
          nation_geo: {
             $geoWithin: {
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
           from: "part",
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
           from: "date",
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
