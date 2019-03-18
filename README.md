# Geo Location Batch Search in Spark

`Spark Version 1.6.2 and Scala Version 2.10 is used for the development of this project.`

## Introduction

This Spark application is written in Scala to map given latitude longitude values to nearest latitude longitude values in a given set using functional programming simulating a map side join of two data sets.  

This takes two sets of data as input. A **master** set of unique id, latitude and longitude values (which are considered constant), and a **search** set of latitude longitudes which are to be searched and mapped to this master set. This is expected to change on every run of the ETL/ join code. **This application is using broadcasting of a sorted index to reduce the need for a cross product using a map side join.**

Main class can be found: *src/main/scala/geosearch/Search.scala*  

Input data files can be found: *src/main/resources/*  

Output data files can be found: *src/main/resources/output/geolocation/*  


## Assumption

* The master data set (id1, lat, long) can be cached in memory, thus not very large. This dataset is used as index. This data set is already sorted (or can be sorted using the code provided) based on latitudes and then optionally on longitudes.
* The master data set has all unique coordinates.

## Mapping logic

The two sets of coordinates are mapped based on the distance calculated by using the **Haversine formula**. The max limits considered as near can be specified while using the utility:

* maxDist - maximum distance that is considered as near (in kilometers) [used 50 here] {note: taken from internet}
* latLimit, lonLimit - To increase efficiency, the area in which the actual distances are calculated is narrowed down by using these limits. 

Eg: 0.5 latLimit means the distance of points beyond searchLat +- 0.5 are ignored, since 0.5 degree means 55.5 Kilometers approx, the mapped lat lon values would not lie beyond this range. Similarly for longitude, till 83 deg latitude the values for 49 Kilometers is 3.9 degree {note: these values are taken from internet}


## Further Optimization Scope

The logic implemented in this project, for optimization first goes through the broadcasted index data and filters out nearest matching Coordinate objects (Id, latitude and longitude value). Here **binary search can be leveraged to reduce complexity.** 


<h2>
<a id="user-content-statement" class="anchor" href="#statement" aria-hidden="true"><span aria-hidden="true" class="octicon octicon-link"></span></a>Statement</h2>
<p>Following are two sets of data which needs to be matched with given criteria.</p>
<p>Both the sets are having 1000 locations nearby to New York City (40.7128, -74.006).
Create a script which reads both the files and combines the nearest two points with given IDs</p>
<h3>
<a id="user-content-data-1csv" class="anchor" href="#data-1csv" aria-hidden="true"><span aria-hidden="true" class="octicon octicon-link"></span></a>data-1.csv</h3>
<pre lang="csv"><code>id1, lat, long
03d3df2c-38d4-40bb-ae71-9ccad52b144c,40.6426473542,-74.3293075111
04347b32-31bb-4557-b246-abe06e0245cc,41.3134347965,-73.4951742093
0435923d-6a74-417f-a954-9b9caf78bb9f,41.2719374754,-74.4852703637
045afec6-b0f5-4818-8b6e-f7a01400ed58,40.7925692352,-73.2985197321
04e2776e-68b0-445e-8034-031814182687,40.7435073569,-73.8177772179
04f5bfea-f504-4760-960e-ca1c7ea8be71,41.0791528292,-73.9510693696
...
</code></pre>
<h3>
<a id="user-content-data-2csv" class="anchor" href="#data-2csv" aria-hidden="true"><span aria-hidden="true" class="octicon octicon-link"></span></a>data-2.csv</h3>
<pre lang="csv"><code>id2, lat, long
0578cb6b-0e60-433f-b055-5866a41c9c0c,40.8325632894,-74.5410619822
0598171a-6e97-41ea-bdac-2d8032e18707,39.8074113214,-74.9934361855
071a56e2-de37-47ab-9486-5ad3f8713853,40.5831982719,-73.841713069
0728e3ab-3107-40bd-9f2a-8bdfbedb85c7,40.9066392291,-74.4597916744
072b2ec5-17a7-4513-a1f0-fb88cb068811,41.427412237,-74.6915754401
0730a9f3-6aec-46b2-90d7-7a54b56e1311,41.3737609473,-74.920673744
...
</code></pre>
<h2>
<a id="user-content-expected-result" class="anchor" href="#expected-result" aria-hidden="true"><span aria-hidden="true" class="octicon octicon-link"></span></a>Expected Result</h2>
<p>Result should be two nearest points. It does not need to be 100% accurate.</p>
<pre lang="csv"><code>id1, id2
03d3df2c-38d4-40bb-ae71-9ccad52b144c,072b2ec5-17a7-4513-a1f0-fb88cb068811
ef2e37a1-150c-4e11-90e4-3a8f462511ec,de6a53b3-8f72-468b-bb01-01375f53e73d
66f6e6c9-0ec8-4de8-b0b9-17e29e0180d9,df8c36cc-3068-4147-809c-732a01287fdc
672348ba-915e-4f9d-8690-940e25442a6d,a1915128-d6b3-4651-b202-e71596edf7fb
a4b9dc59-dbbf-4bef-b256-5cf2a13e02cb,5e17ad7b-279a-4707-9905-e1b968cabd6b
342e062c-609a-4ae9-b35e-1d9f0a8d5a75,2c39b13d-6d8d-4f37-a352-299e5499039d
c5a9e8ef-1542-47cf-b402-e55a10bee0a2,8e703242-3e0e-422a-80ca-a2d14a8fcf28
5189ef67-569b-447c-9ff5-587cf6e8112a,09f3a914-6394-4ab6-945d-fdcd7480506a
079e9a50-11fe-4caf-873a-ba9b90097c65,9e5b19d8-f34b-4367-8036-fcd34c44d2d7
ca64dab9-3fc7-413e-829f-10f93fe9037c,151f6952-c29f-4c15-be25-571ab59ce7b9
b903a14a-4909-4eab-b242-966643efebe5,68dee967-8da1-41e9-9352-2482161ffc3a
...
</code></pre>
