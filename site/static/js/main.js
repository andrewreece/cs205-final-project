/* GLOBALS */	

var doPrint = false; // for testing
var spinner; // for the spinner object
var layout; // plot.ly layout
var step2; // allows for toggle of past/present chart configuration
var past_options_set = false; // listener for when to reveal the step1 div
var full_data_loaded = false;
var closure = true; // for starting and stopping time
var default_starting_score = 7.0; // for series with no zero-point data, this is an arbitrary starting point
var views = { // for toggling between intro and chart views
			 "intro-view": "#container-intro",
			 "chart-view": "#container-chart"
			};


/* HELPER FUNCTIONS */


//////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// FUNCTION: startSpinner(where)
// ARGS: 	 where: String object, spinner can load either mid-page or over step3 div
// PURPOSE:  displays #loading div, spinner animation during AJAX queries
//
//////////////////////////////////////////////////////////////////////////////////////////////////////////
function startSpinner(where) {

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
	var now = "One moment, loading sentiment tracker...";
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

		$("#time-until-debate")
		   .countdown(new Date(nearest_debate.datetime), function(event) {
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


function revealStep1() {
	var interval_id = setInterval(
						function() {
							if (past_options_set) {
								$('#step1').slideDown("slow");
								clearInterval(interval_id);
							}
						}, 200);
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
	$(views[next]).fadeToggle("slow");
}


/* END LAYOUT FUNCTIONS */


/* PLOTTING FUNCTIONS */

function goChart(graph_data) {

	var selected = $('#past-debates').find('option:selected');
	var party    = selected.data('party');
	var date     = selected.data('date');

	var yr, mon, day;  tmp = date.split("_");  yr = tmp[0];  mon = tmp[1];  day = tmp[2];

	layout = {
	  title: 'Twitter sentiment: {0} {1} {2} {3} debate'.format( toTitleCase(mon),day,yr,party.toUpperCase() ),
	  titlefont: {size:24,color:'rgb(57,136,234)'},
	  margin: {t:100},
	  dragmode: "pan",
	  xaxis: {title:"Time (30 second intervals)",type:"date",showgrid:false},
	  yaxis: {title:"Average Tweet Happiness"}
	};

	Plotly.newPlot('chart', graph_data, layout, {displaylogo: false, scrollZoom: true});
	
	var $moredata = $('<div>', {'id':'more-data-coming'} );
	$moredata.css({'position':'absolute',
					'top':$('#chart').offset().top+60,
					'margin-left': 'auto',
					'margin-right': 'auto',
					'left':0,
					'right':0,
					'width':500,
					'height':35,
					'z-index':2000,
					'display':'inline-block',
					'font-size':'11pt',
					'color':'rgb(193,35,35)'
				  });
	var tinyload = "<div id='tiny-loading' style='position:relative;z-index:2001;width:20px;height:20px;display:inline-block;margin-left:15px;'></div>";
	var moredata_text = "More data is on its way! For now, feel free to explore what's here. ";
	$moredata.html(moredata_text+tinyload);

	$('#chart').append($moredata);

	startSpinner('tiny-loading');
}


function extractFeatures(rawdata, tmp, getrange) {

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
		}

		var millisecond_tstamp = parseInt(tstamp)*1000;
		tmp[candidate_name].x.push( millisecond_tstamp );

		var sentiment_score = (i===0) ? default_starting_score : json.sentiment_avg;
		tmp[candidate_name].y.push( sentiment_score );
	});

	if (getrange) { return [rangemin, rangemax]; }
}


function buildChartSeries(tmpdata,master) {

	Object.keys(tmpdata).forEach( function(cname) {
		master.push( 
			{ 	"name": 		cname,
			  	"x": 			tmpdata[cname].x,
			  	"y": 			tmpdata[cname].y,
			  	"type": 		"scatter",
			  	"connectgaps": 	true 
			});
		});
}


function goOboe(start,end) {

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

				Plotly.redraw(chart); 

				full_data_loaded = true;
			}
		);
}


function movingUpdate() {

	var intid = setInterval( 

		function() {

			if (!closure) {
				console.log('no closure, shifting graph');
				Object.keys(chart.data).forEach( function(k) {
					console.log('before length:'+chart.data[k].x.length);
					chart.data[k].x.shift();
					console.log('after length:'+chart.data[k].x.length);
					chart.data[k].y.shift();
				});

				console.log('redrawing now!');
				Plotly.redraw(chart);
			} else {
				console.log('clearing movingupdate interval now');
				clearInterval(intid);
			}
		}, 1000
	);
}

function loadChartData(selected) {

	var start_time 	 	= selected.data('start_time');
	var end_time 	 	= selected.data('end_time');
	var initial_span  	= 1200;
	var init_end_time  	= start_time + initial_span;
	var json_file		= '/get_debate_data/{0}/{1}'.format(start_time,init_end_time);

	console.log('start time: '+start_time+', end time: '+end_time);
	$('#container-intro').fadeTo(300,0.1);
	startSpinner('loading');

	d3.json(json_file, function(rawdata) {
		
		var d3_graphdata = {};
		var d3_series 	 = [];

		extractFeatures(rawdata, d3_graphdata, false);

		buildChartSeries(d3_graphdata,d3_series);

		spinner.stop();
        $("#loading").fadeToggle(200);
        $('#container-intro').fadeTo(100,1.0);
		goChart(d3_series);
		swapViews("intro-view","chart-view");
	});

	goOboe(start_time,end_time);
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
			loadChartData(selected);
		}
	);

$('#start-time').click(
	function() { closure = (closure) ? false : true; movingUpdate(closure); }
);

$('#start-over').click(
	function() { swapViews("chart-view","intro-view"); }
);
