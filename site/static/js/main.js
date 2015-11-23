var cluster_id;
var baking_done = false;
var start_peeking = false;
var color = "red";
var check_interval = 30000;
var tweet_interval = 5000;
var interval_id, interval_id2;
var table_name = "tweettest";
var ct = 0;
var max_ct = 40;

function bakeIt() {
	d3.json('/bake', function(data) {
		cluster_id = data.Cluster.Id;
		$('#bake-report').html( "Starting cluster...stand by for reporting<br />" );
		start_peeking = true;
		shouldWeCheckYet(start_peeking,cluster_id);
	});
}

function shouldWeCheckYet(start_peeking,cid) {
	if (start_peeking && !baking_done) {
		interval_id = setInterval( function() { ovenPeek(cid); }, check_interval); 
	} else {
		console.log('Not yet!');
	}
}
function ovenPeek(cid) {
	console.log('cid: '+cid);
	d3.json('/checkcluster/'+cid, function(data) {
		if ((data.status=="WAITING") || (data.status=="RUNNING")) {
			baking_done=true;
			$('#bake-report').html( $('#bake-report').html()+"<br /><br />TIME TO TAKE OUT THE SNACKS." );
			getData(table_name);
			clearInterval(interval_id);
		} else {
			$('#bake-report').html( function(d) { 

				if (color == "red") {
					color = "blue";
				} else {
					color = "red";
				}
				return $('#bake-report').html()+"<br /><span style='color:"+color+";'>"+JSON.stringify(data)+"</span>"; } );
		}
	});
}

function getData(table_name) {
	
	$('#tweet').html("Working backwards from latest:<br />");
	interval_id2 = setInterval( 
		function() { 
			if (ct < max_ct) {
				ct++;
				d3.json('/pull/'+table_name, function(error,data) {
					//console.log(data);
					display(data,ct);
				});
			} else {
				clearInterval(interval_id2);
				console.log('stopping tweet pull');	
			}
	}, tweet_interval);
	
}

function display(d,back) {
	var data_len = d3.entries(d).length;
	var ix = data_len - 1; //use data_len - back if you want each iteration to move back one tweet in time
	d3.select("#tweet").html( $('#tweet').html()+"<br /><br />"+JSON.stringify(d3.entries(d)[ix].value) );
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

$('#pull').click( function() { $('#tweet').html("One moment <br />"); getData(table_name); } );
$('#bake').click( function() { bakeIt(); } );
$('#already-baking-check').click( function() { 
	var cluster_id = $('#cid').val();
	$('#bake-report').text("Just a moment!...checking");
	shouldWeCheckYet(true,cluster_id);
} );
