# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 15:37:22 2017

@author: cmunoz
"""

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.mime.image import MIMEImage


def generate_report(report_data, reports_path, filtered_log_file):  
    with open(reports_path + filtered_log_file + '.rep', 'a') as f:
        f.write('Data Access Analytics - Summary Report\n\n')
        f.write('Period first date: ' + report_data['date_first'] + '\n')
        f.write('Period last date: ' + report_data['date_last'] + '\n\n')
        f.write('Total data accesses: ' + report_data['total_data_accesses'] + '\n')
        f.write('Total number of unique users: ' + report_data['total_unique_users'] + '\n')
        f.write('Total number of countries: ' + report_data['total_countries'] + '\n\n')
        f.write('Total GB data transferred: ' + report_data['GB_total_data_access'] + '\n')
        f.write('GB data downloads as NetCDF: ' + report_data['GB_netcdf_download'] + '\n')
        f.write('GB data accessed through opendap: ' + report_data['GB_opendap_access'] + '\n\n')


def send_report_through_email(report_data, figs_path, filtered_log_file, img_path): 
    fromaddr = "cmunoz@socib.es"
    #toaddr = ["cmunoz@socib.es", "data.centre@socib.es", "jtintore@socib.es", "jallen@socib.es", "eheslop@socib.es"]
    toaddr = "cmunoz@socib.es"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    #msg['To'] = ", ".join(toaddr)
    msg['To'] = toaddr
    msg['Subject'] = 'SOCIB Data Access Analytics  -- ' + report_data['date_first'] + ' to ' + report_data['date_last'] 

    html = ('''<html>
            <head>
            <style>
            table, th, td {
                border: 0px solid black;
                border-collapse: collapse;
                border-bottom: 1px solid #ddd;
                background-color: #FFFAFA;
                padding: 8px;
                text-align: left;

            } 
            
            div.relative {
                position: relative;
                width: 840px;
                border:1px solid #D8D8D8;
                background-color: white;
                padding:8px; 
                border-radius: 5px;
            } 
            
            div.absolute {
                position: absolute;
                top: 80px;
                left:500px;
                width: 200px;
                height: 100px;
            }
        
            </style>
            </head>
            <body>
            <div class="relative">
                <p style="text-align:left; margin-top:0px; margin-bottom:0px; padding:0px;"><img src="cid:image6" alt="image"/></p>
                <br><h2>Data Access Analytics</h2></br>
                <p><b>Provider: SOCIB Data Center</b></p>
                <p>This report provides analytics for SOCIB external data access. The metrics are gathered from the log files obtained
                from the Apache applications server that contains the Thredds (Thematic Real-time Environmental Distributed Data Services)
                data server.</p>
                <br><br><hr width="830" align="left" color="#D8D8D8" size="0.5"></br></br>
                ''' + 
                '<p><h3>SUMMARY</h3></p>' +
                '<br>Report Period :  ' + '<b>' + report_data['date_first'] + ' to ' + report_data['date_last'] + '</b></br>' +
                '<p>Total data accesses :  ' + '<b>' + report_data['total_data_accesses'] + '</b></p>' + 
                '<p>Total number of unique users :  ' + '<b>' + report_data['total_unique_users'] + '</b></p>' +
                '<p>Total GB data transferred :  ' + '<b>' + report_data['GB_total_data_access'] + '</b></p>' +
                '<p>GB data downloads as NetCDF :  ' + '<b>' + report_data['GB_netcdf_download'] + '</b></p>' +
                '<p>GB data accessed through opendap :  ' + '<b>' + report_data['GB_opendap_access'] + '</b></p>' +
                '<p>Total number of countries :  <b>' + report_data['total_countries'] + '</b></p>' +
                '<br><br><hr width="830" align="left" color="#D8D8D8" size="0.5"></br></br>' +
                
                '<p><h3>DATA ACCESS</h3></p>' +
                '<br><h3>Data accesses comparison</h3></br>' +
                '<p>Total volume of data transferred in giga-bytes (GB) determined using two main Data Access Services provided by the THREDDS data server:</p>' +
                '<p><ul>' +
                '<li><b>OPeNDAP</b>: Makes local data remotely accessible by clients.</li>' +
                '<li><b>File Server</b>: Files are accessed through the HTTP file download.</li>' +
                '</p></ul>' +
                '<p><img src="cid:image1" width="400" height="300"></p>' +
                
                '<br><h3>Data Access per Platform Type</h3></br>' +
                '<p>Total volume of data transferred (GB) per platform type. The platform types are the following:</p>' + 
                '<p><ul>'  +
                '<li><b>animal</b>: sea-turtles.</li>' +
                '<li><b>auv</b>: Gliders fleet.</li>' +
                '<li><b>drifter</b>: Lagrangian platforms including surface drifters and Argo Profilers.</li>' +
                '<li><b>hf_radar</b>: High Frequency Radar stations.</li>' +
                '<li><b>mooring</b>: Fixed stations including sea-level, meteorological, surface moorings, underwater moorings and coastal stations.</li>' +
                '<li><b>operational_models</b>: operational forecasting datasets including ocean, meteo-tsunamis and waves data.</li>' +
                '<li><b>research_vessel</b>: Reserch vessel including CTD, VM ADCP, Meteo, Position and Thermosalinometer.</li>' +
                '</p></ul>' +
                '<p><img src="cid:image2" width="400" height="350"></p>' +                
                '<br><h3>Top 10 Most Accessed Stations by Unique Users</h3></br>' +
                '<p>' + report_data['stations_access_html'] + '</p>' +
                
                '<br><h3>Top 10 Most Accessed Files by Unique Users</h3></br>' +
                '<p>' + report_data['files_access_html'] + '</p>' +
                '<br><br><hr width="830" align="left" color="#D8D8D8" size="0.5"></br></br>' +
                
                '<p><h3>GENERAL USERS</h3></p>' +
                '<br><h3>Data Access for Top 10 Countries</h3></br>' +
                '<p>The volume of data transferred (GB) by the top ten fraction of the ' + report_data['total_countries'] + 
                ' countries of origin of the users that accessed SOCIB datasets during the period: ' 
                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
                '<p><img src="cid:image4" width="400" height="300"></p>' +
                
                '<br><h3>Top 20 Most Active Users</h3></br>' +
                '<p>The top twenty of the ' + report_data['total_unique_users'] +
                ' unique users that accessed SOCIB datasets during the period: ' 
                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' + 
                '<p>' + report_data['general_users_html'] + '</p>' +
                '<br><br><hr width="830" align="left" color="#D8D8D8" size="0.5"></br></br>' +

                '<p><h3>TRENDINGS</h3></p>' +
                '<p>General monthly trendings related with users and data access.</p>'                
                '<br><h3>Users</h3></br>' +
#                '<p>Total volume of data transferred (GB) per tracked organization during the period: ' 
#                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
                '<p><img src="cid:image7" width="700" height="450"></p>' +
                
                '<br><h3>Data Access</h3></br>' +
#                '<p>Total number of single accesses per tracked organization during the period: ' 
#                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
                '<p><img src="cid:image8" width="700" height="450"></p>' +                
#                '<p><h3>TRACKED USERS</h3></p>' +
#                '<p>Results applied to the following tracked organizations that accessed SOCIB datasets during the period: ' 
#                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
#                '<p><ol>'  +
#                '<li><b>sasemar</b>: Sociedad de Salvamento y Seguridad Marítima.</li>' +
#                '<li><b>puertos_del_estado</b>: Ente Público Empresarial Puertos del Estado.</li>' +
#                '<li><b>mongoos</b>: Mediterranean Operational Network for the Global Ocean Observing System (MONGOOS).</li>' +
#                '<li><b>ieo</b>: Instituto Español de Oceanografía (IEO).</li>' +
#                '<li><b>emodnet</b>: European Marine Observation and Data Network (EMODnet).</li>' +
#                '<li><b>csic</b>: Consejo Superior de Investgaciones Científicas (CSIC).</li>' +
#                '</p></ol>' +
#                
#                '<br><h3>Data Access per Tracked Organization</h3></br>' +
#                '<p>Total volume of data transferred (GB) per tracked organization during the period: ' 
#                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
#                '<p><img src="cid:image3" width="700" height="450"></p>' +
#                
#                '<br><h3>Number of Single Accesses to Data per Tracked Organization</h3></br>' +
#                '<p>Total number of single accesses per tracked organization during the period: ' 
#                + report_data['date_first'] + ' to ' + report_data['date_last'] + '.</p>' +
#                '<p><img src="cid:image5" width="700" height="450"></p>' +
             


            '</div>' +
                ''' 

            </body>
            </html>''').encode('utf-8')
            
    body = MIMEText(html, 'html')
    #msgText = MIMEText('<b>%s</b><br><img src="cid:%s"><br>' % (body, attachment), 'html') 
    msg.attach(body)   # Added, and edited the previous line
    attachment = figs_path + filtered_log_file + '/data_access.png'    
    attachment2 = figs_path + filtered_log_file + '/platform_data.png'     
    #attachment3 = figs_path + filtered_log_file + '/total_GB_transferred_per_tracked_ip.png'
    attachment4 = figs_path + filtered_log_file + '/total_countries.png'
    #attachment5 = figs_path + filtered_log_file + '/total_num_accesses_per_tracked_ip.png'
    attachment6 = img_path + 'socib.png'
    attachment7 = figs_path + filtered_log_file + '/users_trends.png'
    attachment8 = figs_path + filtered_log_file + '/data_access_trends.png'
    
    
    fp = open(attachment, 'rb') 
    fp2 = open(attachment2, 'rb')          
    #fp3 = open(attachment3, 'rb') 
    fp4 = open(attachment4, 'rb') 
    #fp5 = open(attachment5, 'rb')  
    fp6 = open(attachment6, 'rb')  
    fp7 = open(attachment7, 'rb')
    fp8 = open(attachment8, 'rb')                                  
    
    img = MIMEImage(fp.read())
    img2 = MIMEImage(fp2.read())
    #img3 = MIMEImage(fp3.read())
    img4 = MIMEImage(fp4.read())
    #img5 = MIMEImage(fp5.read())
    img6 = MIMEImage(fp6.read())
    img7 = MIMEImage(fp7.read())
    img8 = MIMEImage(fp8.read())
    
    fp.close()
    fp2.close()
    #fp3.close()
    fp4.close()
    #fp5.close()
    fp6.close()
    fp7.close()
    fp8.close()
    
    img.add_header('Content-ID', '<image1>'.format(attachment))
    img2.add_header('Content-ID', '<image2>'.format(attachment2))
    #img3.add_header('Content-ID', '<image3>'.format(attachment3))
    img4.add_header('Content-ID', '<image4>'.format(attachment4))
    #img5.add_header('Content-ID', '<image5>'.format(attachment5))
    img6.add_header('Content-ID', '<image6>'.format(attachment6))
    img7.add_header('Content-ID', '<image7>'.format(attachment7))
    img8.add_header('Content-ID', '<image8>'.format(attachment8))
    #img.add_header('Content-ID', '<{}>'.format(attachment)) #this attaches the image instead of embedding it
    msg.attach(img)
    msg.attach(img2)
    #msg.attach(img3)
    msg.attach(img4)
    #msg.attach(img5)
    msg.attach(img6)
    msg.attach(img7)
    msg.attach(img8)
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "password")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()   

        
        
        
