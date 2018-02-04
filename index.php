<?php
session_start();
?>
<html>
<head><title>CMS Usage of Physics Datasets</title>
<link rel="stylesheet" type="text/css" href="plotstyle.css">
</head>
<body>
<font color=#000000 size=+1>
  <br>
<table width="100%"> 
  <tr>
    <td>
      <h1>
        <a href="index.html">Data Popularity</a>
        <br>
        <span style="font-size: 15pt">
          [Historical Summary]
        </span>
      </h1>
    </td>
  </tr>
</table>
<hr>
<center>
<table width="50%"> 
  <tr>  <th id="left"> Time period </th>  <th id="left"> T1 Only </th> <th id="left"> T2 Only </th>                                   <th id="left"> T1+T2 </th>                                                 <th id="left"> Text files </th>                                                  </tr>
  <tr> <td> Ending today </td> <td> <a href="latest/now/T1.xlsx">T1.xlsx</a> </td> <td> <a href="latest/now/T2.xlsx">T2.xlsx</a> </td> <td> <a href="latest/now/T12.xlsx">T12.xlsx</a> </td> <td> <a href="latest/now/txt/">dump</a> </td> </tr>
  <tr> <td> Ending 20171231 </td> <td> <a href="latest/end2017/T1.xlsx">T1.xlsx</a> </td> <td> <a href="latest/end2017/T2.xlsx">T2.xlsx</a> </td> <td> <a href="latest/end2017/T12.xlsx">T12.xlsx</a> </td> <td> <a href="latest/end2017/txt/">dump</a> </td> </tr>
  <?php
    $dirs = glob('2*');
    foreach ($dirs as $d) {
      echo "<tr> <td> Ending $d </td> <td> <a href=\"$d/now//T1.xlsx\">T1.xlsx</a> </td> <td> <a href=\"$d/now//T2.xlsx\">T2.xlsx</a> </td> <td> <a href=\"$d/now//T12.xlsx\">T12.xlsx</a> </td> <td>  </td> </tr>\n";
      }
  ?>
</table>
<br>
<br>
<a href="http://t3serv001.mit.edu/~snarayan/daily_popularity/IN-14-XXX_temp.pdf">Documentation</a>
</center>
<br>
<hr>
</body>
</html>
