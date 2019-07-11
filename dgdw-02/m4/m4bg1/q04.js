var n = db.nation.findOne({nation_pk:2});

var result = db.city.aggregate([
   {
      $match: {
          city_geo: {
             $geoWithin: {
                $geometry: n.nation_geo
             }
          }
      }
   },
   { $project : { "city_pk": 1 } }, 
   
   {	
       $lookup:
       {
           from: "customer",
           localField: "city_pk",
           foreignField: "c_city_fk",
           as: "customer_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$customer_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
   { $project : { "customer_join.c_custkey": 1 } },   
   
   
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "customer_join.c_custkey",
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

