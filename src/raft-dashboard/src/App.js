import { TableContainer, Table, TableHead, TableBody, TableRow, TableCell, Paper } from '@mui/material';
import React, { useEffect, useState } from 'react';
import axios from 'axios';
import './App.css';

function App() {

  const termBgColors = [ '#f44336', '#e91e63', '#9c27b0', '#673ab7', '#3f51b5', '#2196f3', '#03a9f4', '#00bcd4', '#009688', '#4caf50' ];
  const leaderBgColor = '#4caf50';
  const followerBgColor = '#9e9e9e';
  const API_PORT = 8090;

  var [rafts, setRafts] = useState([]);
  var [logs, setLogs] = useState([]);

  useEffect(() => {
    getRafts();

    const interval = setInterval(() => {
      getRafts();
    }, 2000); // 2 seconds
    
    return () => clearInterval(interval);
  }, []);

  const getRafts = async () => {
    try {
      const response = await axios.get(`http://localhost:${API_PORT}/raft`, {timeout: 100000});
      console.log("resp: ", response.data);
      setRafts(response.data);
      console.log("raft: ", rafts);
    }
    catch (error) {
      console.log(error);
    } 
  }

  // const getLogs = async () => {
  //   try {
  //     logs = [];
  //     for (var raft of rafts.listPortFollower) {
  //       console.log(raft);
  //       // const response = await axios.get(`http://localhost:${API_PORT}/log/${raft}`);   //TODO: change to actual endpoint
  //       // logs.push(response)
  //     }
  //   }
  //   catch (error) {
  //     console.log(error);
  //   }
  // };

  //  DUMMY DATA
      // logs = [
      //   {
      //     address: '255.255.255.1:3000',
      //     type: 'leader',
      //     log: [
      //       { term: 1, log: 'enqueue 1' },
      //       { term: 1, log: 'enqueue 2' },
      //       { term: 2, log: 'dequeue 3' },
      //       { term: 3, log: 'enqueue 4' },
      //       { term: 3, log: 'enqueue 5' },
      //       { term: 3, log: 'enqueue 6' },
      //       { term: 4, log: 'enqueue 7' },
      //       { term: 5, log: 'enqueue 8' },
      //       { term: 5, log: 'enqueue 9' },
      //       { term: 6, log: 'enqueue 10' },
      //       { term: 7, log: 'enqueue 11' },
      //     ]
      //   },
      //   {
      //     address: '132.233.32.1:3000',
      //     type: 'follower',
      //     log: [
      //       { term: 1, log: 'enqueue 1' },
      //       { term: 2, log: 'enqueue 2' },
      //     ]
      //   },
      //   {
      //     address: '255.121.255.1:3000',
      //     type: 'follower',
      //     log: [
      //       { term: 1, log: 'enqueue 1' },
      //       { term: 1, log: 'enqueue 2' },
      //       { term: 2, log: 'dequeue 3' },
      //     ]
      //   },
      // ];

  function getMaxLogLength() {
    var max = 0;
    logs.forEach(node => {
      if (node.log.length > max) {
        max = node.log.length;
      }
    });
    return max;
  };

  function isLeaderNode(node) {
    return node.type === 'leader';
  }

  return (
    <div className="App">
      <h1>Raft Dashboard</h1>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell align="center" style={{ position: 'sticky', left: 0, zIndex: 1, background: 'white' }}>Address</TableCell>
              {Array.from(Array(getMaxLogLength()).keys()).map((i) => (
                <TableCell align="center">{i + 1}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rafts.map((node) => (
              <TableRow style={{ background: '#' }}>
                {isLeaderNode(node)? 
                <TableCell align="center" style={{ position: 'sticky', left: 0, zIndex: 1, background: leaderBgColor }}>{node.address}</TableCell> 
                : 
                <TableCell align="center" style={{ position: 'sticky', left: 0, zIndex: 1, background: followerBgColor }}>{node.address}</TableCell>
                }
                {node.log.map((logentry) => (
                  <TableCell align="center">
                    <div className='term' style={{ background: termBgColors[(logentry.term) % termBgColors.length], color: 'white' }}>Term {logentry.term}</div>
                    <div className='log'>{logentry.executionType} {logentry.value}</div>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <br />
      <div className='legend'>
        <h4 style={{ textAlign: 'left' }}>Legend:</h4>
        <div style={{ background: leaderBgColor, maxWidth: '150px' }}>Leader Node</div>
        <div style={{ background: followerBgColor, maxWidth: '150px' }}>Follower Node</div>
      </div>
    </div>
  );
}

export default App;
