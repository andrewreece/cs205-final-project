/* GLOBALS */	

var doPrint = false; // for testing
var spinner; // for the spinner object
var layout; // plot.ly layout
var step2; // allows for toggle of past/present chart configuration
var past_options_set = false; // listener for when to reveal the step1 div
var full_data_loaded = false;
var ready_indicator; // set from call to gd_cluster_running/, indicates whether live data is ready to go
var closure = true; // for starting and stopping time
var default_starting_score = 7.0; // for series with no zero-point data, this is an arbitrary starting point
var onload_now = new Date();
var maximum_time_span = 240; // in the live streaming case, how many minutes from now to keep pulling data?
var rounding_interval = 30; // round timestamps to interval floor. eg. interval=30, 8:30:28 --> 8:30:00
var master_chart_data = []; // keeps track of growing chart data in the streaming case
var every_N_milliseconds = 30000;
var timezone_offset = new Date().getTimezoneOffset()*60*1000;
var fetch_round = 0; // tracks number of times from initial request we've fetched data. resets on new request.
var views = { // for toggling between intro and chart views
			 "intro-view": "#container-intro",
			 "chart-view": "#container-chart"
			};


/* HELPER FUNCTIONS */

function startSpinner(where) {
	/* displays #loading div, spinner animation during AJAX queries */
	// see: http://fgnass.github.io/spin.js/
	var opts = {"loading": 	{
								lines: 13, // The number of lines to draw
								length: 20, // The length of each line
								width: 10, // The line thickness
								radius: 30, // The radius of the inner circle
								corners: 1, // Corner roundness (0..1)
								rotate: 0, // The rotation offset
								direction: 1, // 1: clockwise, -1: counterclockwise
								color: '#5287B3', // #rgb or #rrggbb or array of colors
								speed: 1, // Rounds per second
								trail: 60, // Afterglow percentage
								shadow: false, // Whether to render a shadow
								hwaccel: false, // Whether to use hardware acceleration
								className: 'spinner', // The CSS class to assign to the spinner
								zIndex: 2e9, // The z-index (defaults to 2000000000)
								top: '80%', // Top position relative to parent
								left: '50%' // Left position relative to parent
							},
				"tiny-loading": {
								lines:9,
								length:7,
								width:3,
								radius:10,
								scale:0.45,
								corners:1.0,
							  	color: '#5287B3', // #rgb or #rrggbb or array of colors
								opacity:0.25,
								rotate:0,
								direction:1,
								speed:1.0,
								trail:60,
							    zIndex: 2e9, // The z-index (defaults to 2000000000)
							    top: '80%', // Top position relative to parent
							    left: '0%' // Left position relative to parent
							}
				};

	$('#'+where).fadeIn();

	var target = document.getElementById(where);
    spinner = new Spinner(opts[where]).spin(target); 

}


function roundToTwo(num) {    
	/* rounds to 2 decimal places */
    return +(Math.round(num + "e+2")  + "e-2");
}


/* 	This isn't really a helper function, but a mod on the String prototype.
	It allows for .format() style string concatenation, a la Python.  It's awesome.
	From: http://stackoverflow.com/questions/610406/javascript-equivalent-to-printf-string-format
*/
if (!String.prototype.format) {
  String.prototype.format = function() {
    var args = arguments;
    return this.replace(/{(\d+)}/g, function(match, number) { 
      return typeof args[number] != 'undefined' ? args[number] : match ;
    });
  };
}

function toTitleCase(str) {
/* 	Converts first letter of a string to uppercase
	http://stackoverflow.com/questions/196972/convert-string-to-title-case-with-javascript
*/
    return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
}

/* END HELPER FUNCTIONS */




/* LAYOUT (NON-CHART) FUNCTIONS */

function getPastDebateOptions() {

	d3.text('/gd_cluster_running', function(raw_now_response) {
		var now_response;

		var tmp = raw_now_response.split("_");
		ready_indicator = tmp[0]; // this is a global variable
		now_response    = tmp[1];

		var now = "<div style='text-align:left;align:left;font-size:12pt;padding-left:20px;'>"+now_response+"</div>";

		var past = '2. Choose a debate: ' + '<select id="past-debates">';
		var options;

		d3.json('/get_debate_options', function(response) {

			response.data.forEach( function(d,i) {

				yr = d.ddate.split("_")[0];
				mon_name = toTitleCase(d.ddate.split("_")[1]);
				day = d.ddate.split("_")[2];

				var selected = (i===0) ? "selected" : "";

				options += '<option id="{0}-{1}" data-date="{1}" data-party="{0}" data-start_time="{6}" data-end_time="{7}" value="{0}-{1}" {8}>{2} {3} {4} ({5})</option>'.format(d.party,d.ddate,mon_name,day,yr,d.party.toUpperCase(),d.start_time,d.end_time,selected);
			});
			past += options;
			past += '</select> '+'<input type="button" id="past-debate-select" value="Start tracking" />';
		    
		    step2 = {"now":now,"past": past};

		    past_options_set = true;
		});
	});
}


function revealStep1() {
	var interval_id = setInterval(
						function() {
							if (past_options_set) {
								$('#step1').slideDown("slow");
								clearInterval(interval_id);
							}
						}, 200);
}


function getNearest(schedules,today) {

	function updateNearest(schedule,distance_from_today, nearest_delta, nearest_debate) {
		// if nearest is in the past, check to see if it's within 3 hrs of now
		// 3hrs = 180min = 180000ms
		// if so, report that debate is currently going on! (and ask if you want to track it)
		var three_hours = 180000;
		if ( Math.abs(distance_from_today) < nearest_delta ) {
			if ( (distance_from_today > 0) || (Math.abs(distance_from_today) <= three_hours) ) {
				nearest_delta = distance_from_today;
				nearest_debate = schedule;
			}
		}
		return [nearest_delta,nearest_debate];
	}

	var nearest_delta = Infinity;
	var nearest_debate;

	schedules.forEach( function(schedule,i) {

		var this_debate_date 	= new Date(schedule.datetime);
		var distance_from_today = this_debate_date - today;
		tmp = updateNearest(schedule, distance_from_today, nearest_delta, nearest_debate);
		nearest_delta  = tmp[0];
		nearest_debate = tmp[1];
	});

	return [nearest_delta, nearest_debate];
}


function updateNextDebateDiv(nearest_delta,nearest_debate) {

	function getPartyColor(party) {
	  	if (party=="Republican") 		{ return "red"; 	} 
	  	else if (party=="Democratic") 	{ return "blue"; 	} 
	  	else 							{ return "purple"; 	}
	}


	if (nearest_delta <=0) {
			$("#next-debate-when").html('Is happening now!');
	} else {
		$("#next-debate-when").html('It starts in <div id="time-until-debate"></div>.');	

		var debate_date_obj = new Date(nearest_debate.datetime);
		debate_date_obj.setMilliseconds(debate_date_obj.getMilliseconds() + timezone_offset);

		$("#time-until-debate")
		   .countdown( debate_date_obj, function(event) {
		     $(this).text(
		       event.strftime('%D days %H hrs %M min %S seconds')
		     );
		   });
	}

	$("#debate-party").html(nearest_debate.party)
					  .css("color", getPartyColor(nearest_debate.party) );

	$("#debate-date").html(nearest_debate.date);
	$("#debate-time").html(nearest_debate.time);

	$('#next-debate-info').fadeIn("slow"); 
	$('#steps').css('display','block');
}


function updateOnTimeframeChange(choice) {

	function updateStep2(content) {
		$('#step2-body').html(content);
		$('#step2').slideDown("slow");
	}

	if ($('#step2').css('display')!="none") {
		$.when( $('#step2').slideUp("slow") )
		 .done( function() {  updateStep2(step2[choice]); } );
	} else {
		updateStep2(step2[choice]);
	}
}


function swapViews(prev,next) {
	$(views[prev]).fadeToggle("slow");
	setTimeout( function() { $(views[next]).fadeToggle("slow"); }, 1000);
	$('#container-controls').fadeToggle("slow");
}


/* END LAYOUT FUNCTIONS */


/* PLOTTING FUNCTIONS */

function showDataComingMsg() {
	var tinyload, moredata_text;
	tinyload = "<div id='tiny-loading'></div>";
	moredata_text = "More data is on its way! For now, feel free to explore what's here. ";

	console.log('fetch round: '+fetch_round);
	console.log('more data coming element:');
	console.log($('#more-data-coming'));
	console.log($('#more-data-coming').length);
	if ($('#more-data-coming').length == 0) {
		$moredata = $('<div>', {'id':'more-data-coming'} );
		$moredata.addClass('more-data-coming');
		$moredata.css('top',$('#chart').offset().top+60);
		$moredata.html(moredata_text+tinyload);
		$('#chart').append($moredata);
	} else {
		tinyload = "<div id='tiny-loading'></div>";
		moredata_text = "More data is on its way! For now, feel free to explore what's here. ";
		$('#more-data-coming').html(moredata_text+tinyload);
		$('#more-data-coming').show();
	}

}

function goChart(graph_data,timeframe) {

	var chart_title;

	if (timeframe=="past") {

		var selected = $('#past-debates').find('option:selected');
		var party    = selected.data('party');
		var date     = selected.data('date');
		var yr, mon, day;  
		var tmp = date.split("_");  
		yr = tmp[0];  mon = tmp[1];  day = tmp[2];

		chart_title = 'Twitter sentiment: {0} {1} {2} {3} debate'.format( toTitleCase(mon),day,yr,party.toUpperCase() );
	
	} else if (timeframe=="now") {

		chart_title = 'Twitter sentiment live stream (updates every {0} seconds)'.format(rounding_interval);
	}

	var chart_layout = {  title: chart_title,
						  titlefont: { 	
						  				size:24,
						  				color:'rgb(57,136,234)'
						  			 },
						  margin: { t:100 },
						  dragmode: "pan",
						  xaxis: { 	title:"Time ({0} second intervals)".format(rounding_interval),
						  			type:"date",
						  			showgrid:false
						  		 },
						  yaxis: { title:"Average Tweet Happiness" }
					   };

	var chart_config = { displaylogo: false, 
						 scrollZoom:  true
					   };

	Plotly.newPlot('chart', graph_data, chart_layout, chart_config);
	
	if (timeframe=="past") {
		showDataComingMsg();
		startSpinner('tiny-loading');
	}

}


function extractFeatures(rawdata, tmp, getrange, timeframe) {

	var rangemin =  Infinity;
	var rangemax = -Infinity;

	Object.keys(rawdata.data).forEach( function(d,i) {

		var json, tstamp, candidate_name;

		rawdata.data[d].forEach( function(dd,i) {
			if (dd.Name == "data") {
				json = JSON.parse(dd.Value.replace(/\bNaN\b/g, "null"));
			} else if (dd.Name == "timestamp") {
				tstamp = dd.Value;
				if (getrange) {
					if (parseInt(tstamp) < rangemin) { rangemin = parseInt(tstamp); }
					if (parseInt(tstamp) > rangemax) { rangemax = parseInt(tstamp); }
				}
			} else if (dd.Name == "candidate") {
				candidate_name = dd.Value;
			}
		});

		if (Object.keys(tmp).indexOf(candidate_name) === -1) {
			tmp[candidate_name] = {};
			tmp[candidate_name].x = [];
			tmp[candidate_name].y = [];
			tmp[candidate_name].std = [];
		}

		
		/* BAD HACK ALERT!

			You discovered a day before this project was due that your streaming data is storing
			non-offsetted timestamps (meaning it shows up in GMT not EST), but your archival data
			stores offsetted timestamps.  That means that if you apply the timezone_offset to all 
			data, it will show the correct (EST) time for streaming but not for archives.

			Currently, you have sloppily solved this with the below conditional statement.
			What you really need to do is go back into the Spark Streaming code and get the
			right timestamp stored when you write to SDB.

		*/
		var offset = (timeframe=="now") ? timezone_offset : 0;

		/* 	X-AXIS: timestamp in milliseconds
			Y-AXIS: sentiment average
			ERROR:  +/- Std Dev (NaN if "average" is based off of one tweet
		*/
		var millisecond_tstamp = parseInt(tstamp)*1000;
		tmp[candidate_name].x.push( millisecond_tstamp + offset);

		var sentiment_score = (json.sentiment_avg > 0) ? json.sentiment_avg : NaN;
		tmp[candidate_name].y.push( roundToTwo(sentiment_score) );

		var sentiment_std = (json.sentiment_std > 0) ? json.sentiment_std : NaN;
		tmp[candidate_name].std.push( roundToTwo(sentiment_std) );
	});

	if (getrange) { return [rangemin, rangemax]; }
}


function buildChartSeries(tmpdata,master) {

	Object.keys(tmpdata).forEach( function(cname) {
		master.push( 
			{ 	"name": 		cname,
			  	"x": 			tmpdata[cname].x,
			  	"y": 			tmpdata[cname].y,
				"error_y": {
							    type: 'data',
							    array: (tmpdata[cname].std) ? tmpdata[cname].std : [0],
							    visible: true
				},
			  	"type": 		"scatter",
			  	"connectgaps": 	true 
			});
		});
}


function getFullData(start,end) {

	var oboe_graphdata 	= {};
	var oboe_series 	= [];
	var oboe_temp 		= { "data":{} };
	var json_file 		= '/get_debate_data/{0}/{1}'.format(start,end);
	var ct 		 = 0;

	oboe(json_file)
		.node( '*', 
				function(d) {

					if ($.isArray(d))  {
						oboe_temp.data[ct.toString()] = d;
						ct++;
					}
				}
		)

		.done(	
			function() {
				console.log('done oboe');
				tmp = extractFeatures(oboe_temp, oboe_graphdata,true);
				var rangemin = tmp[0];
				var rangemax = tmp[1];
				buildChartSeries( oboe_graphdata, oboe_series );

				chart.data = oboe_series;
				chart.layout.xaxis.range = [rangemin,rangemax];

				spinner.stop();

				$('#more-data-coming').html('Data has arrived!').css({'padding-left':100}).delay(1000).fadeOut(200);
				$('#more-data-coming').hide();

				master_chart_data = oboe_series;

				Plotly.redraw(chart); 
				$('#error-bar-toggle').prop('checked', true);

				full_data_loaded = true;
			}
		);
}


function updateMasterSeries(master,fresh) {

	//console.log('this is fresh');
	//console.log(fresh);

	if (master.length > 0) {

		//console.log('this is master');
		//console.log(master);

		fresh.forEach( function(cand_data) {
			var found = false;
			master_chart_data.forEach( function(mdata,i) {
				//console.log('i: '+i);
				//console.log('mdata:');
				//console.log(mdata);
				if (mdata.name==cand_data.name) {
					master_chart_data[i].x.push.apply(mdata.x, cand_data.x);
					master_chart_data[i].y.push.apply(mdata.y, cand_data.y);
					master_chart_data[i].error_y.array.push.apply(mdata.error_y.array, cand_data.error_y.array);
					found = true;
				}  
			});

			if ( (!found) && (!isNaN(cand_data.y[0])) ) {
				console.log('new name found, here is cand_data.y[0]');
				console.log(cand_data.y[0]);
				//console.log('here is current master chart data:');
				//console.log(master_chart_data);
				var tmpdata = {};
				tmpdata[cand_data.name] = cand_data;
				buildChartSeries(tmpdata,master_chart_data);
			}
		});

	} else { // initial assignment
		master_chart_data = fresh;
	}
}


function getDebateData(json_file,timeframe) {
	d3.json(json_file, function(rawdata) {
		
		var d3_graphdata = {};
		var d3_series 	 = [];

		extractFeatures(rawdata, d3_graphdata, false);
		buildChartSeries(d3_graphdata,d3_series);
		updateMasterSeries(master_chart_data,d3_series);

	    if (fetch_round==1) {
	    	spinner.stop();
	    	$("#loading").fadeOut(200);
		}
		goChart(master_chart_data,timeframe);

		if (fetch_round==1) {
	        $('#container-intro').fadeTo(100,1.0);
			swapViews("intro-view","chart-view");
		}
	});
}


function updateChart(original_start, end_time_secs, timeframe, interval_id) {
	fetch_round++;

	var now = new Date();
	var sec = now.getSeconds();

	var floor_sec = Math.floor( sec / rounding_interval ) * rounding_interval;

	start_time_milli 	= now.setSeconds(floor_sec);
	start_time_secs 	= Math.floor( (now.getTime() - timezone_offset) / 1000 );
	start_time_oneback 	= start_time_secs - rounding_interval; // get previous N second interval of data

	if (start_time_secs < end_time_secs) {

		console.log('current batchtime (30s back): '+start_time_oneback);

		var json_file = '/get_debate_data/{0}/{1}'.format(start_time_oneback,0);

		getDebateData(json_file,timeframe);

	} else {
		console.log('original start time: '+original_start);
		console.log('start time: '+new Date(start_time_milli)+' end time: '+new Date(end_time_secs*1000));
		console.log('start time secs: '+start_time_secs+', end time secs: '+end_time_secs);
		console.log('closing loop now.');
		clearInterval(interval_id);
	}
}


function loadChartData(obj,timeframe) {
	/* Note: obj is not the same data type, depending on whether it's coming from a request for
			 streaming or archived data. If archived, it's the DOM <option> object of the debate date selected.
			 If streaming, it's a date object. 
			 This is not particularly good form. But since there is no equivalent of a select object to pass in
			 in the streaming case, we were faced with either passing in an empty dict, so, same data type but
			 empty content, or passing in a different data type with useful information.
			 There's probably a better way to go about doing this.
	*/
	var start_time, end_time;
	if (fetch_round) { fetch_round=0; }

	$('#container-intro').fadeTo(300,0.1);
	startSpinner('loading');

	if (timeframe=="past") {

		fetch_round++;

		start_time = obj.data('start_time');
		end_time   = obj.data('end_time');
		console.log('start time: '+start_time+', end time: '+end_time);

		/* If we need to grab a full debate's worth of data, that's too big to load quickly.
			So instead we load the first 1200 seconds (20 min) while we have Oboe.js get the rest
		*/
		var initial_span  	= 1200; 
		var init_end_time  	= start_time + initial_span;
		var json_file		= '/get_debate_data/{0}/{1}'.format(start_time,init_end_time);

		getDebateData(json_file,timeframe);
		getFullData(start_time,end_time);

	} else if (timeframe=="now") {

		var interval_id 	= null;
		var original_start  = new Date();

		obj.setMinutes(obj.getMinutes()+maximum_time_span);

		var end_time_secs = Math.floor( (obj.getTime() - timezone_offset) / 1000 );

		updateChart(original_start, end_time_secs, timeframe);

		interval_id = setInterval( 
			function() {
				updateChart(original_start, end_time_secs, timeframe, interval_id);
			}, every_N_milliseconds);
	}

}

/* END PLOTTING FUNCTIONS */


/* BEGIN EVENT LISTENERS */

/* SETUP: On page load, get info about next upcoming debate, reveal options */
$(document).ready(function() {

	$homelink = $('a',{'href':'http://gaugingdebate.com'});
	$homelink.css(
		{  'position':'absolute',
			'display':'block',
			'top':$('#title').offset().top,
			'left':$('#title').offset().left,
			'width':'30px',
			'height':'30px',
			'cursor':'pointer'
		});
	d3.csv(	'/getschedule', 

			function(schedules) {

				var today = new Date();

				getPastDebateOptions();

				tmp = getNearest(schedules,today);
				var nearest_delta = tmp[0];
				var nearest_debate = tmp[1];

				updateNextDebateDiv(nearest_delta, nearest_debate);

				revealStep1();
			}
	);
});

/* 	STEP 1: User selects streaming or archived charts */
$('input[name=timeframe]')
	.click( 
		function() {
			updateOnTimeframeChange( $(this).val() );
		}
	);


/* 	STEP 2: IF User choose archived charts, then User selects a specific debate

	NOTE: Here we listen for #past-debate-select click, NOT for #step2-body click!
	it's a 'deferred' assignment, as #past-debate-select does not exist on page load.
	(this is the way to assign click events to dynamically created elements with jQuery)
	for more see: http://stackoverflow.com/questions/6658752/click-event-doesnt-work-on-dynamically-generated-elements 
*/
$('#step2-body')
	.on('click', '#past-debate-select', 
		function() {
			var selected = $('#past-debates').find('option:selected');
			loadChartData(selected,"past");
		}
	);

$('#step2-body')
	.on('click', '#start-live-tracking,#override-live-tracking',
		function() {
			loadChartData(new Date(),"now");
		}
	);

$('#start-time').click(
	function() { closure = (closure) ? false : true; movingUpdate(closure); }
);

$('#start-over').click(
	function() { 
		master_chart_data = [];
		swapViews("chart-view","intro-view"); 
		$('#error-bar-toggle').prop('checked', true);
		//$('error-bar-toggle').attr('checked', 'checked');
	}
);

$('#error-bar-toggle').click(
	function() {
		var show_bars = this.checked;
		master_chart_data.forEach( function(d,i) {
			master_chart_data[i].error_y.visible = show_bars;
		});
		chart.data = master_chart_data;
		Plotly.redraw(chart);
	}
	);
