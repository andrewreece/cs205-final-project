debates = {"gop":["sep16"],"dem":["oct13"]}
num_debates = np.concatenate(np.array(debates.values())).shape[0]

ct = 0
domain_name = "debates"
for party in debates:
    for ddate in debates[party]:
        ct+=1
        client.put_attributes(
                DomainName= domain_name,
                ItemName  = str(ct),
                Attributes= [{'Name':'party','Value':party},{'Name':'ddate','Value':ddate}]
            ) 