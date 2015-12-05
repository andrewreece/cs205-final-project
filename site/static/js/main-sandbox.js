/* GLOBALS */	
var cluster_id;						// Global so terminate() has access
var color = "red";					// Toggle, alternates blue/red output on cluster status report (testing only)
var check_interval = 30000;			// How frequently should we check up on a baking cluster?
var tweet_interval = 5000;			// How frequently should we pull down new data from SDB?
var interval_id, interval_id2;		// setInterval IDs (we need these to stop them)
var table_name = "tweettest";		// SDB table name (we may end up having more than one for LDA, sentiment, etc)
var ct = 0;							// ct and max_ct keep track of how many tweets we've displayed in our output
var max_ct = 40;					// "" ""
var tnames=['tweets','sentiment']; 	// SDB table names 
var bake_starting_msg = "Starting cluster...stand by for reporting<br />";
var bake_complete_msg = "CLUSTER IS FULLY BAKED. DATA COMING.";

// from SO: http://stackoverflow.com/questions/610406/javascript-equivalent-to-printf-string-format
// mimics .format() in python
if (!String.prototype.format) {
  String.prototype.format = function() {
    var args = arguments;
    return this.replace(/{(\d+)}/g, function(match, number) { 
      return typeof args[number] != 'undefined'
        ? args[number]
        : match
      ;
    });
  };
}

function startPeeking(cid, interval) {
	/* Wraps setInterval() loop for checking on cluster status */
	interval_id = setInterval( 
					function() { ovenPeek(cid); }, 
					interval ); // We need interval_id to stop loop
}

function bake(interval) {
	/* Spin up a new cluster, then initiate status check loop 
	   Note: /bake hits run.py, returns json status.  See run.py documentation for more.
	*/
	d3.json('/bake', function(data) {
		$('#bake-report').html( bake_starting_msg );
		cluster_id = data.Cluster.Id;
		startPeeking(cluster_id, interval); 
	});
}

function ovenPeek(cid) {
	/* Periodically check on currently-baking cluster, report on status */
		// testing
		// console.log('cid: '+cid);

	// /checkcluster hits run.py, see run.py for more
	d3.json('/checkcluster/'+cid, function(data) {

		// If RUNNING: May see old data for the first few returns, as new data hasn't gone to DB yet.
		// If WAITING: Should see only new data, cluster jobs have completed.
		if ((data.status=="WAITING") || (data.status=="RUNNING")) {

			getData(tnames); 		// pull down SDB data
			clearInterval(interval_id);	// stop cluster status check loop

			// Print "all done" status
			$('#bake-report').html( $('#bake-report').html()+"<br /><br />"+bake_complete_msg );

		// If NOT (RUNNING OR WAITING): Cluster is still starting up, report status and keep looping
		} else {

			$('#bake-report').html( function(d) { 

				color = (color == "red") ? "blue" : "red"; // Toggle print colors, no good reason
				span_front = "<br /><span style='color:"+color+";'>";
				span_back  = "</span>";
				new_html   = span_front + JSON.stringify(data) + span_back;
				return $('#bake-report').html()+new_html; 
			});
		}
	});
}

function getData(table_names) {
	/* Pulls data down from SDB, via Flask
			- Hits run.py, see run.py for more
			- Currently returns unprocessed tweets (only a bit of field filtering from Spark)
		This in mainly the output funciton for the initial PoC...we'll replace this soon.
		Note: ct, max_ct, tweet_interval are globals!
	*/
	$('#tweet').html("Working backwards from latest:<br />");
	interval_id2 = setInterval(  // We need interval_id2 to stop loop
		function() { 

			// max_ct is an arbitrary number, determines how many tweets to display as output
			if (ct < max_ct) {

				ct++;
				console.log('/pull/'+table_names[0]);
				console.log('/pull/'+table_names[1]);
				d3.json('/pull/'+table_names[0], function(error,data) {
					console.log(data);
					var data_len = d3.entries(data).length;
					var ix = data_len - 1;
					var new_html = "<br /><br />"+JSON.stringify(d3.entries(data)[ix].value);
					d3.select("#tweet").html( $('#tweet').html()+new_html );
				});

				d3.json('/pull/'+table_names[1], function(error,data) {
					var data_len = d3.entries(data).length;
					var ix = data_len - 1;
					var new_html = "<br /><br />"+JSON.stringify(d3.entries(data)[ix].value);
					d3.select("#sentiment").html( $('#sentiment').html()+new_html );
				});
			// If we reach maximum number of test outputs, stop the loop, reset ct
			} else {

				clearInterval(interval_id2);
				console.log('stopping tweet pull');	
				ct = 0;
			}
		}, 
		tweet_interval); // How frequently should we check the database?
	
}

function display(d,back) {
	/* Once we start using D3, we may want to wrap our more complex display commands in a function 
		( Currently we just do all our output writes in getData() )
	*/

	//console.log(Object.keys(d));
	/*
	d3.selectAll(".tweet")
		.data(d3.entries(d))
		.enter()
		.append("div")
			.attr("class","tweet")
			.style("width","500px")
			.style("height","100px")
			.style("display",function(d,i) { if (i==(data_len-2)) {return "block";} else {return "none";}})
			.style("border","solid 2px maroon")
			.text( function(d,i) { return JSON.stringify(d.value); });
	*/
}

function terminate(cid) {
	/* Terminate EMR cluster (need cluster ID) */
	d3.text( '/terminate/'+cid, function(data) {
		// test output to console
		console.log(data);
		$('#terminate-report').text(JSON.stringify(data));
	});
}




/* 
	Below are HTML containers with onclick triggers to make things happen 
*/

// Get data from SDB
$('#pull').click( function() { $('#tweet').html("One moment <br />"); getData(tnames); } );

// Start a new cluster
$('#bake').click( function() { bake(check_interval); } );

// Check on an existing cluster
$('#already-baking-check').click( function() { 
	cluster_id = $('#cid').val(); // This button requires an ID to work properly. No error handling yet!
	$('#bake-report').text("Just a moment!...checking");
	startPeeking(cluster_id, check_interval);
} );

// Terminate current cluster
$('#terminate').click( function() { terminate(cluster_id); } );

// Terminate different cluster
$('#terminate-other').click( function() { 
	cluster_id = $('#terminate-cid').val();
	terminate(cluster_id); 
} );



// http://stackoverflow.com/questions/196972/convert-string-to-title-case-with-javascript
function toTitleCase(str) {
    return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
}

var step2;
function getPastDebateOptions() {
	var now = "One moment, loading sentiment tracker...";
	var past = '2. Choose a debate: ' + '<select id="past-debates">';
	var options;
	d3.json('/get_debate_options', function(response) {
		response.data.forEach( function(d,i) {
			yr = d.ddate.split("_")[0];
			mon_name = toTitleCase(d.ddate.split("_")[1]);
			day = d.ddate.split("_")[2];
			start_time = d.start_time;
			end_time = d.end_time;
			var selected = (i===0) ? "selected" : "";
			options += '<option id="{0}-{1}" data-start_time="{6}" data-end_time="{7}" value="{0}-{1}" {8}>{2} {3} {4} ({5})</option>'.format(d.party,d.ddate,mon_name,day,yr,d.party.toUpperCase(),start_time,end_time,selected);
		});
		past += options;
		past += '</select> '+'<input type="button" id="past-debate-select" value="Start tracking" />';
	    
	    step2 = {"now":now,"past": past};
	});
}

step1_set = false;

// gets info about upcoming debate
$(document).ready(function() {
	d3.csv('/getschedule', function(schedules) {
		var today = new Date();
		var nearest_delta = Infinity;
		var nearest_debate;
		schedules.forEach( function(s,i) {
			var d = new Date(s.datetime);
			var distance_from_today = d-today;
			// if nearest is in the past, check to see if it's within 3 hrs of now
			// 3hrs = 180min = 180000ms
			// if so, report that debate is currently going on! (and ask if you want to track it)
			if ( Math.abs(distance_from_today) < nearest_delta ) {
				if ( (distance_from_today > 0) || (Math.abs(distance_from_today) <= 180000) ) {
					nearest_delta = distance_from_today;
					nearest_debate = s;
				}
			}
		});
		if (nearest_delta <=0) {
			$("#next-debate-when").html('Is happening now!');
		} else {
			$("#next-debate-when").html('It starts in <div id="time-until-debate"></div>.');	

			$("#time-until-debate")
			   .countdown(new Date(nearest_debate.datetime), function(event) {
			     $(this).text(
			       event.strftime('%D days %H hrs %M min %S seconds')
			     );
			   });
		}
		
		$("#debate-party").html(nearest_debate.party)
						  .css("color",function() { 
						  	if (nearest_debate.party=="Republican") {
						  		return "red";
						  	} else if (nearest_debate.party=="Democratic") {
						  		return "blue";
						  	} else {
						  		return "purple";
						  	}
						  });
		$("#debate-date").html(nearest_debate.date);
		$("#debate-time").html(nearest_debate.time);

		$('#next-debate-info').slideDown("slow"); 
		 var timeout_id = setTimeout(function() {
		 								$('#step1').slideDown("slow");
		 								step1_set = true;
		 								clearTimeout(timeout_id);
		 							}, 600);
		 
	});

	getPastDebateOptions();

});


function updateStep2(content) {
	$('#step2-body').html(content);
	$('#step2').slideDown("slow");
}

$('input[name=timeframe]').click( function() {
	var choice = $('input[name=timeframe]:checked').val();
	if ($('#step2').css('display')!="none") {
		$.when( $('#step2').slideUp("slow") )
		 .done( function() { 
		 	updateStep2(step2[choice]); 
		 	console.log(choice);
		 } );// if not wrapped in anonymous, then starts early
	} else {
		updateStep2(step2[choice]);
	}
});


var palette = new Rickshaw.Color.Palette();

function goRickshaw(series) {
var t0 = performance.now();

	var graph = new Rickshaw.Graph( {
	        element: document.querySelector("#chart"),
	        width: 640,
	        height: 380,
	        renderer: 'line',
	        series: series,
	        min:6,
	        max:9
	} );

	var x_axis = new Rickshaw.Graph.Axis.Time( { 
		graph: graph,
		timeFixture: new Rickshaw.Fixtures.Time.Local(),
		orientation: 'bottom'
	} );

	x_axis.render();

	var y_axis = new Rickshaw.Graph.Axis.Y( {
	        graph: graph,
	        orientation: 'left',
	        tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
	        element: document.getElementById('y_axis')
	} );
	y_axis.render();

	var legend = new Rickshaw.Graph.Legend( {
	        element: document.querySelector('#legend'),
	        graph: graph
	} );

	var offsetForm = document.getElementById('offset_form');

	graph.render();


	var hoverDetail = new Rickshaw.Graph.HoverDetail( {
		graph: graph,
		xFormatter: function(x) { return new Date(x*1000).toLocaleString(); }
	} );

	var shelving = new Rickshaw.Graph.Behavior.Series.Toggle( {
		graph: graph,
		legend: legend
	} );

var t1 = performance.now();
console.log("Call to goRickshaw took " + (t1 - t0) + " milliseconds.");
}
var sentiment_data = {};
var graphdata = {};
var fin = false;
var series = [];

$('#step2-body').on('click', '#past-debate-select', function() {

var t0 = performance.now();
	var target = $('#past-debates').val();
	var selected = $('#past-debates').find('option:selected');
	var start_time = selected.data('start_time');
	var final_end_time = selected.data('end_time');

	console.log('start time: '+start_time+', end time: '+end_time);
	var time_lag = 600;
	var time_increment = 30;
	var current_end_time = start_time + time_lag;



//define function to load all pages
function loadAllData(url, callback) {
    var data = [];
    var count = 0; //just for demo

    //define recursive function to process each page
    function loadNext(urlNext) {
        console.log('loading page ' + urlNext);
        
        d3.json(urlNext, function (error, json) {
            if (error) {
                alert("Cannot access data");
                callback(error); //sends back D3 error object
                return; //stop executing this function
            }

            count += json.results.length;
           
            console.log(count + ' records loaded');

		Object.keys(json.data).forEach( function(d) {
			var jdata;
			var candidate_name = d.split("_")[0];
			var tstamp = d.split("_")[1];
			
			if (Object.keys(sentiment_data).indexOf(candidate_name) === -1) {
				sentiment_data[candidate_name] = {};
				graphdata[candidate_name] = [];
			}
			data.data[d].forEach( function(dd,i) {
				if (dd.Name == "data") {
					jdata = JSON.parse(dd.Value.replace(/\bNaN\b/g, "null"));
				} 
			});
			sentiment_data[candidate_name][jdata.timestamp.toString()] = jdata;
			//console.log(tstamp);
			//var x_val = new Date(parseInt(tstamp)*1000);
			//console.log(x_val);
			graphdata[candidate_name].push( {"x":parseInt(tstamp),"y":jdata.sentiment_avg} );
			
		});
		Object.keys(graphdata).forEach( function(cname) {
			series.push( {"name":cname,"data":graphdata[cname],"color":palette.color()} );
		});


            var times = urlNext.split("/");

            var previous_end_time = times[2];
            var new_start_time = previous_end_time + time_increment;
            var current_end_time = ((new_start_time + time_lag) > final_end_time) ? final_end_time : new_start_time + time_lag;
            var next_url = url +'{0}/{1}'.format(new_start_time,current_end_time);
            //If there is another page, call loadNext again (recursion)                   
            if (previous_end_time != current_end_time) {
                loadNext(next_url);
            } else {
                console.log('no more pages to load');
                fin = true;
                //execute callback
                callback(series);
            }
        });

    }
    url = url+'{0}/{1}'.format(start_time,current_end_time);
    //call recursive function
    loadNext(url);

}

//call the function to load all pages
loadAllData('/get_debate_data/', function (allData) {
    console.log("this will run after the callback has been called");
    console.log(allData);
});

	var interval_id = 	setInterval( 
							function() { 
								if (fin) { 
									console.log('all done!'); 
									//console.log(series); 
									goRickshaw(series);

									var t1 = performance.now();
									console.log("Call to d3.json took " + (t1 - t0) + " milliseconds.");

									clearInterval(interval_id); 
								} 
							},
						500);
});


/*
$('#test-oboe').click(function() {
	oboe('/get_debate_data/1442446200/1442460600')
		.node('*', function(d) {
			console.log(d);
		});
});
*/

