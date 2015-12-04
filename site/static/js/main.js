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
			options += '<option id="{0}-{1}" data-start_time="{6}" data-end_time="{7}" value="{0}-{1}" {8}>{2} {3} {4}({5})</option>'.format(d.party,d.ddate,mon_name,day,yr,d.party.toUpperCase(),start_time,end_time,selected);
		});
		past += options;
		past += '</select> '+'<input type="button" id="past-debate-select" value="Start tracking" />';
	    
	    step2 = {"now":now,"past": past};
	});
}


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

var sentiment_data = {};
var graphdata = {};
var fin = false;
var series = [];
$('#step2-body').on('click', '#past-debate-select', function() {
	var target = $('#past-debates').val();
	var selected = $('#past-debates').find('option:selected');
	var start_time = selected.data('start_time');
	var end_time = selected.data('end_time');

	d3.json('/get_debate_data/{0}/{1}'.format(start_time,end_time), function(data) {
		var ct = 0;

		Object.keys(data.data).forEach( function(d) {
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

			graphdata[candidate_name].push( {"x":tstamp,"y":jdata.sentiment_avg} );
			
			
		});
		Object.keys(graphdata).forEach( function(cname) {
			series.push( {"name":cname,"data":graphdata[cname],"color":palette.color()} );
		});
		fin = true;
	});

	var interval_id = setInterval( 
		function() { 
			if (fin) { 
				clearInterval(interval_id); 
				console.log('all done!'); 
				console.log(sentiment_data); 
				console.log(series);
			} 
		},
		300);
});



// BEGIN RICKSHAW


var graph = new Rickshaw.Graph( {
        element: document.querySelector("#chart"),
        width: 540,
        height: 240,
        renderer: 'line',
        series: [
                {
                        name: "Northeast",
                        data: [ { x: -1893456000, y: 25868573 }, { x: -1577923200, y: 29662053 }, { x: -1262304000, y: 34427091 }, { x: -946771200, y: 35976777 }, { x: -631152000, y: 39477986 }, { x: -315619200, y: 44677819 }, { x: 0, y: 49040703 }, { x: 315532800, y: 49135283 }, { x: 631152000, y: 50809229 }, { x: 946684800, y: 53594378 }, { x: 1262304000, y: 55317240 } ],
                        color: palette.color()
                },
                {
                        name: "Midwest",
                        data: [ { x: -1893456000, y: 29888542 }, { x: -1577923200, y: 34019792 }, { x: -1262304000, y: 38594100 }, { x: -946771200, y: 40143332 }, { x: -631152000, y: 44460762 }, { x: -315619200, y: 51619139 }, { x: 0, y: 56571663 }, { x: 315532800, y: 58865670 }, { x: 631152000, y: 59668632 }, { x: 946684800, y: 64392776 }, { x: 1262304000, y: 66927001 } ],
                        color: palette.color()
                },
                {
                        name: "South",
                        data: [ { x: -1893456000, y: 29389330 }, { x: -1577923200, y: 33125803 }, { x: -1262304000, y: 37857633 }, { x: -946771200, y: 41665901 }, { x: -631152000, y: 47197088 }, { x: -315619200, y: 54973113 }, { x: 0, y: 62795367 }, { x: 315532800, y: 75372362 }, { x: 631152000, y: 85445930 }, { x: 946684800, y: 100236820 }, { x: 1262304000, y: 114555744 } ],
                        color: palette.color()
                },
                {
                        name: "West",
                        data: [ { x: -1893456000, y: 7082086 }, { x: -1577923200, y: 9213920 }, { x: -1262304000, y: 12323836 }, { x: -946771200, y: 14379119 }, { x: -631152000, y: 20189962 }, { x: -315619200, y: 28053104 }, { x: 0, y: 34804193 }, { x: 315532800, y: 43172490 }, { x: 631152000, y: 52786082 }, { x: 946684800, y: 63197932 }, { x: 1262304000, y: 71945553 } ],
                        color: palette.color()
                }
        ]
} );

var x_axis = new Rickshaw.Graph.Axis.Time( { graph: graph } );

var y_axis = new Rickshaw.Graph.Axis.Y( {
        graph: graph,
        orientation: 'left',
        tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
        element: document.getElementById('y_axis'),
} );

var legend = new Rickshaw.Graph.Legend( {
        element: document.querySelector('#legend'),
        graph: graph
} );

var offsetForm = document.getElementById('offset_form');

offsetForm.addEventListener('change', function(e) {
        var offsetMode = e.target.value;

        if (offsetMode == 'lines') {
                graph.setRenderer('line');
                graph.offset = 'zero';
        } else {
                graph.setRenderer('stack');
                graph.offset = offsetMode;
        }       
        graph.render();

}, false);

graph.render();
