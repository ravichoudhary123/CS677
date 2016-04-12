python peer.py 1 10007 '{"Role": "Buyer","Inv":{},"shop":["Fish","Salt","Boar"]}'  7 &
python peer.py 2 10008 '{"Role": "Seller","Inv":{"Fish":0,"Boar":3,"Salt":0},"shop":{}}' 7 &
python peer.py 3 10009 '{"Role": "Buyer","Inv":{},"shop":["Salt","Boar","Fish"]}' 7 & 
python peer.py 4 10010 '{"Role": "Seller","Inv":{"Fish":3,"Boar":0,"Salt":0},"shop":{}}' 7 &  
python peer.py 5 10011 '{"Role": "Buyer","Inv":{},"shop":["Boar","Fish","Salt"]}' 7 &
python peer.py 6 10012 '{"Role": "Seller","Inv":{"Fish":0,"Boar":0,"Salt":3},"shop":{}}' 7 &
python peer.py 7 10013 '{"Role": "Seller","Inv":{"Fish":0,"Boar":0,"Salt":3},"shop":{}}' 7 &