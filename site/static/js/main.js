var cluster_id;
var baking_done = false;
var start_peeking = false;
var color = "red";

function bakeIt() {
	d3.json('/bake', function(data) {
		console.log(data);
		console.log(data.Cluster.Id);
		cluster_id = data.Cluster.Id;
		$('#bake-report').text( function(d) { return JSON.stringify(data); } );
		start_peeking = true;
		setInterval( function() { shouldWeCheckYet(cluster_id); }, 5000);
		console.log('end of bakeIt json()');
	});
}

function shouldWeCheckYet(cid) {
	if (start_peeking && !baking_done) {
		console.log('Now we can peek.');
		ovenPeek(cid);
	} else {
		console.log('Not yet!');
	}
}
function ovenPeek(cid) {
	console.log('cid: '+cid);
	d3.json('/checkcluster/'+cid, function(data) {
		console.log(data);
		console.log(data.status);
		if (data.status=="RUNNING") {
			baking_done=true;
			$('#bake-report').text( "TIME TO TAKE OUT THE SNACKS." );
			getData('test');
		} else {
			$('#bake-report').css("color",function() { 
				if (color == "red") {
					color = "blue";
				} else {
					color = "red";
				}
				return color;
			})
			.text( function(d) { return JSON.stringify(data); } );
		}
	});
}

function getData(table_name) {
	//console.log('in getData');
	// make now playing carousel
	d3.json('/pull/'+table_name, function(error,data) {
		//console.log(data);
		display(data);
	});
	
}

function display(d) {
	var data_len = d3.entries(d).length;
	console.log("length of data: "+data_len);
	d3.select("#tweet").text( JSON.stringify(d3.entries(d)[data_len-2].value) );
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

$('#pull').click( function() { getData('test'); } );
$('#bake').click( function() { bakeIt(); } );
$('#already-baking-check').click( function() { 
	var cluster_id = $('#cid').val();
	setInterval( function() { ovenPeek(cluster_id); }, 15000); 
} );
