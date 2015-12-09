var cluster_id;						// Global so terminate() has access
var color = "red";					// Toggle, alternates blue/red output on cluster status report (testing only)
var check_interval = 30000;			// How frequently should we check up on a baking cluster?
var tweet_interval = 5000;			// How frequently should we pull down new data from SDB?
var interval_id, interval_id2;		// setInterval IDs (we need these to stop them)
var table_name = "tweettest";		// SDB table name (we may end up having more than one for LDA, sentiment, etc)
var ct = 0;							// ct and max_ct keep track of how many tweets we've displayed in our output
var max_ct = 40;					// "" ""
var tnames=['tweets','sentiment']; 	// SDB table names 
var master_type = 'c3_xlarge'; 		// Argument to bake/ for adjusting EMR master node type
var core_type   = 'm1_medium';		// Argument to bake/ for adjusting EMR core node type
var stream_duration = 30;			// Argument to bake/ for adjusting how long the stream stays open
var bake_starting_msg = "Starting cluster...stand by for reporting<br />";
var bake_complete_msg = "CLUSTER IS FULLY BAKED. DATA COMING.";


function startPeeking(cid, interval) {
	/* Wraps setInterval() loop for checking on cluster status */
	interval_id = setInterval( 
					function() { ovenPeek(cid); }, 
					interval ); // We need interval_id to stop loop
}

function bake(interval,mtype,ctype,sduration) {
	/* Spin up a new cluster, then initiate status check loop 
	   Note: /bake hits run.py, returns json status.  See run.py documentation for more.
	*/
	if (mtype === null) { mtype = '_'; }
	if (ctype === null) { ctype = '_'; }

	var path = '/bake/'+mtype+'/'+ctype+'/'+sduration;

	console.log(path);
	d3.json(path, function(data) {
		console.log(data);
		console.log((typeof data));
		$('#bake-report').html( bake_starting_msg );
		if (data === null) {
			alert("You can't use this kind of instance type in an EMR cluster. Restarting...");
			location.reload();
		} else {
			cluster_id = data.Cluster.Id;
			startPeeking(cluster_id, interval); 
		}
	});
}

function ovenPeek(cid) {
	/* Periodically check on currently-baking cluster, report on status */

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

$('#change-defaults').change(

	function() {
		console.log(this.checked);
		if (this.checked) {
			$('#update-settings').slideDown();
		} else {
			$('#update-settings').slideUp();
		}
	}
);


$('#master-type').change( function() {
	master_type = $(this).val().replace(".","_");
});


$('#core-type').change( function() {
	core_type = this.val().replace(".","_");
});

$('#stream-duration').change(function() {
	stream_duration = this.val();
});

// Get data from SDB
$('#pull').click( function() { $('#tweet').html("One moment <br />"); getData(tnames); } );

// Start a new cluster
$('#bake').click( function() { bake(check_interval,master_type,core_type); } );

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


/* ONLOAD SETUP */
$(document).ready(function() {

	// get instance types from text file, load into dropdown box options
	var path = '/static/js/instance-types.txt';
	var options = {"null":"Instance type"};

	d3.text(path, function(data) {
		var arr = data.split(",");
		arr.forEach( function(d) {
			options[d]=d;
		});
		var $mtype = $('#master-type');
		var $ctype = $('#core-type');

		$.each(options, function(key, value) {   
		     $mtype.append($("<option/>", {
		         value:key,
		         text:value
		     }));
		     $ctype.append($("<option/>", {
		         value:key,
		         text:value
		     }));
		});	
	});

	var $duration = $('#stream-duration');
	var mins = _.range(5,250,5); // 5 to 245 min (~4hr max)
	
	d3.text('/get_duration', function(val) {
		var default_duration = val;

		$('#default-duration').text(default_duration);

		$.each(mins, function(i,min) {   
		     $duration.append($("<option/>", {
		         value:min,
		         text:min+" min",
		         selected: function() { if (min==default_duration) {return true;} else { return false; } }
		     }));
		 });
	});
	
});

