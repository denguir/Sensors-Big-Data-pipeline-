# Big Data Management and Analytics System (BDMAS) for Smart cities.

The Brussels government, wanting to lay claim to the title of smart city, has initiated a project where it will equip a modest number of households and public buildings with such sensors. The project consists in implementing a Big Data Management & Analytics (BDMA) platform that will store, manage and analyze this sensor data.
The project is divided into two phases:
1. In Phase I, you need to design the Big Data Management pipeline that will be used to store and manage the sensor data, as well as implement certain batch and streaming queries for the pipeline in order to show its feasibility. `The report can be found in the docs folder`.
2. In Phase II, you will employ Machine Learning methods on actual sensor data in order to build a model of the data that can be used for predictions.

## Big Data Management pipeline (Phase I)
Consists in implementing batch and streaming queries to show feasibility.

### Data

- **Spaces**: Represent a physical space.
    - ID : 
    - META-DATA: 
    - For each participating space the identifiers of the sensors located in the space must be stored, as well as their coordinates x,y w.r.t "base coordinate".
    - 10000 spaces involved

- **Sensors**:
    - ID
    - Municiplality ID
    - Types:
        - Temperatures: Float C
        - Humidity: FLoat %
        - Light: Float Lux
        - Movement: Boolean
    - Voltage: indicating the battery level
    - For each sensor we must store its type.
    - Send that +/- every 30 seconds.

Data / min = # spaces x # sensors x 60s/30s

#### Research Lab Example

![research_lab](images/research_lab.jpg)

- Divided into 54 points
    - Each point equipped with 3 sensors:
        1. p-0 : temperature
        2. p-1 : humidity
        3. p-2 : light
    - total number of sensors = 54 x 3 = 162

#### Log Format

- Date: yyyy-mm-dd, 2019-02-01, First of February 2019

- Time: hh:mm:ss.xxxx, 10:15:10.1234, 10 am 15 minutes 10.1234 seconds

- SensorID: p-i, p belongs [1-54], i belongs [0-2]

- Measurement: real number

- Voltage: real number in volts

`Note`: sometimes sensors does not report a measurement each 30 seconds.

### Requirements

- Develop a BDMA pipeline
    - Store Data
    - Manage Data
- Web based dashboard

#### Queries

1. Statistics (Max, Min, AVG) of sensor readings per type. Can be grouped by:
    - Granularity in space
        - per space / municipality/ entirely Brussels
    - Granularity in time
        - Last x hours / days / months / years
    - Ex. Max **temperature value** in the **research Lab** in the **last 5 hours**.

2. For each slot of 15 minutes in a day 
    - If average temperature over +/- 30 measurements >= 19 C : daytime
    - If average temperature over +/- 30 measurements < 19 C : nighttime
    - Explanation:
        - 24h / day
        - 15 minutes slots
        - total = 96 slots / day
        - Ex. 00h00 to 00h15, corresponds to the first slot of the day
    - Grouping : results can be **"Rolled up"**
        - time
            - last n days of x min/ hours slots
            - last n weeks of x min / hours/ days slots
            - last n months of x min / hours / days / weeks slots
            - last n years of x min / hours / days / weeks / months slots
        - space
            - all sensors in a specific space / municipality / entire Brussels
3. Overview of frequent temperature values within 1 hour sliding window over the data stream.
    - Set frequent threshold, Ex. theta = 100
    - Can consider 19.439 and 19.42 the same measurement (different decimals).

#### Considerations

- Streaming Data +/- every 30 seconds
- Query and analysis should be implemented in **Spark Streaming**
- Save data for the past 10 years
- Real time dashboard 
- Describe in the report which sets of technologies we will use and explain why.
- Research about alternatives to lambda-architecture and evaluate which one we will use and justify why.
- Specify in the report any assumption about the data.
- Design a scalable platform and do an analysis of the expected data volumes in function of the number of sensors per space.

### Deliverables

- `Deadline`
    - Friday, Third of may 2019

- **Code**
    - README file
    - Requirements for installation
- **Report**
    - General
        - Analysis
        - Motivation
        - Description of design
    - Specific
        - Overall architecture BDMA pipeline (Ex. Lambda-architecture)
        -  For each component of the architecture
            - Description of functional purposes
            - Discussion of the set of possible technologies
            - Technology that was selected with a motivation
            - Any additional assumption w.r.t. data set
            - Data volumes analysis
            - For each query a description of how they were implemented
            - Description of the Dashboards implementation and a screenshot
    

### Installation

Requirements: docker & docker-compose 

### How to Use

- On localhost:

	Step 1: Lauch all Services:
		- Run "docker-compose up -d" under /code/kafka-datagen directory
		- A juptyter notebook should be available on localhost:8888 and localhost:8889

	Step 3: Run the Speed Layer:
		- Type localhost:8888 in your favorite browser, than open a New Terminal
		- Then, execute "python app.py"

	Step 4: Run the Batch Layer:
		- Type localhost:8889 in your favorite browser, than open a New Terminal
		- Then, execute "cd notebooks" and "python app.py"

	Step 5: Visualize the data:
		- Go to localhost:3000, a Grafana Dashboard should be available
		- Login as admin (password: admin)
		- You can then create your own dashboards under "CREATE NEW DASHBOARD"

	Step 6: Perform a query:
		- Choose "Add query"
		- Under Metric, type a metric name (example: 'temperature')
		- Specify an aggregator (example: 'max')
		- You can specify a Downsample window (example: 1m) with an aggregator (example: 'avg')
		- Under Filters, you can filter by space and aggregators, example:
			city = iliteral_or(Brussels) , groupBy = true  
			_aggregate = iliteral_or(MAX) , groupBy = false
		- The result of this query gives you the MAX temperature for each 1-minute time window

- On our Server (http://52.91.196.166)

	Steps 1 to 4 are already done (everything already running):
		- You can visit the SpeedLayer Running terminal on http://52.91.196.166:8888
		- You can visit the BatchLayer Running terminal on http://52.91.196.166:8889
	Step 5 and 6:
		- Same as for localhost case

### Additional notes about Grafana

`user: admin`  
`pass: admin`

You can already access the following created dashboards:

- [Time (temperature)](http://52.91.196.166:3000/d/e94z7zmZk/time-temperature?orgId=1&from=1556825867230&to=1556912267230)
- [Brussels, Humidity sensors](http://52.91.196.166:3000/d/-L0bWkmZk/brussels-humidity-sensors?orgId=1&from=1556826113343&to=1556912513343)
- [Brussels, Motion sensors](http://52.91.196.166:3000/d/v89KmziZz/brussels-motion-sensors?orgId=1&from=1556890947100&to=1556912547100)
- [Brussels, light sensors](http://52.91.196.166:3000/d/GvPAzkmWk/brussels-light-sensors?orgId=1&from=1556891166726&to=1556912766727)
- [Brussels, temperature sensors](http://52.91.196.166:3000/d/d4mVwkmZk/frequent-temperature-brussels?orgId=1&from=1556826412058&to=1556912812058)

The available Metrics are:
- temperature
- humidity
- light
- motion
- temperature.occurence
- occurence.temperature

Note that you can also filter by other space granularities:
- space: from 1 to 54
- municipality: from 0 to 18
- city: Brussels (only available)

You can also use other aggregation operation following your query
	


