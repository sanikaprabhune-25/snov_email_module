import React, { useState, useEffect, useRef } from 'react';
import io from 'socket.io-client';
import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

// 7 days in seconds for long-lived ping
const seven_days = 7 * 24 * 60 * 60;

// Configuration for easy deployment port changes
const BACKEND_HOST = 'ec2-3-110-132-80.ap-south-1.compute.amazonaws.com';
const BACKEND_PORT = '8000';

//http://ec2-65-2-83-4.ap-south-1.compute.amazonaws.com:5000
const socket = io(`http://${BACKEND_HOST}:${BACKEND_PORT}`, {
  transports: ['polling'],
  pingInterval: seven_days,
  pingTimeout: seven_days,
});

const styles = {
  container: {
    padding: '40px',
    maxWidth: '1000px',
    margin: '30px auto',
    fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    background: '#f5f7fa',
    borderRadius: '10px',
    boxShadow: '0px 4px 20px rgba(0, 0, 0, 0.1)',
  },
  tabContainer: {
    display: 'flex',
    justifyContent: 'center',
    marginBottom: '30px',
    borderBottom: '2px solid #e1e4e8',
  },
  tabButton: (active) => ({
    flex: 1,
    padding: '15px 0',
    background: 'transparent',
    color: active ? '#2c3e50' : '#7f8c8d',
    border: 'none',
    borderBottom: active ? '4px solid #3498db' : 'none',
    cursor: 'pointer',
    fontSize: '18px',
    fontWeight: active ? '600' : '400',
    transition: 'color 0.3s, border-bottom 0.3s',
  }),
  card: {
    padding: '30px',
    background: '#ffffff',
    borderRadius: '8px',
    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
    marginBottom: '20px',
  },
  input: {
    width: '100%',
    padding: '12px',
    marginBottom: '16px',
    borderRadius: '6px',
    border: '1px solid #dfe3e8',
    fontSize: '16px',
  },
  textarea: {
    width: '100%',
    padding: '12px',
    marginBottom: '16px',
    borderRadius: '6px',
    border: '1px solid #dfe3e8',
    fontSize: '16px',
    resize: 'vertical',
  },
  button: (bgColor, disabled) => ({
    width: '100%',
    padding: '14px',
    backgroundColor: disabled ? '#bdc3c7' : bgColor,
    color: '#fff',
    border: 'none',
    borderRadius: '6px',
    cursor: disabled ? 'not-allowed' : 'pointer',
    fontSize: '16px',
    marginBottom: '16px',
    transition: 'background-color 0.3s',
  }),
  progressContainer: {
    marginTop: '20px',
    padding: '15px',
    background: '#ecf0f1',
    borderRadius: '6px',
  },
  progressBox: {
    border: '1px solid #bdc3c7',
    borderRadius: '5px',
    padding: '10px',
    marginBottom: '10px',
    textAlign: 'center',
    fontSize: '16px',
    fontWeight: '600',
    color: '#34495e',
  },
  statusRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '20px',
  },
  statusText: {
    fontSize: '18px',
    fontWeight: '500',
  },
  logContainer: {
    marginTop: '20px',
    background: '#fefefe',
    padding: '15px',
    borderRadius: '6px',
    maxHeight: '300px',
    overflowY: 'auto',
    border: '1px solid #ddd',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  tableHeader: {
    backgroundColor: '#3498db',
    color: '#fff',
    padding: '12px',
    textAlign: 'left',
  },
  tableCell: {
    padding: '12px',
    borderBottom: '1px solid #f1f1f1',
  },
};

function FileUpload() {
  const [activeTab, setActiveTab] = useState('upload');
  const [designations, setDesignations] = useState('');
  const [location, setLocation] = useState('');
  const [numResults, setNumResults] = useState('');
  const [arrayInput, setArrayInput] = useState('');
  const [domains, setDomains] = useState('');
  const [overallProgress, setOverallProgress] = useState({ total: 0, processed: 0, remaining: 0 });
  const [progressUpdates, setProgressUpdates] = useState([]);
  const previewSet = useRef(new Set());
  const [previewData, setPreviewData] = useState([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [downloadFileName, setDownloadFileName] = useState('');

  useEffect(() => {
    socket.on('connection_rejected', ({ message }) => {
      alert(message);
      socket.disconnect();
      window.close();
    });

    socket.on('process_data_response', (data) => {
      toast.success(data.message);
      setIsProcessing(false);
    });

    socket.on('refresh_response', (data) => {
      toast.info(data.message);
      setDesignations('');
      setLocation('');
      setNumResults('');
      setArrayInput('');
      setDomains('');
      setOverallProgress({ total: 0, processed: 0, remaining: 0 });
      setProgressUpdates([]);
      previewSet.current.clear();
      setPreviewData([]);
      setIsProcessing(false);
    });

    socket.on('overall_progress', (data) => {
      setOverallProgress({
        total: data.total,
        processed: data.processed,
        remaining: data.remaining,
      });
    });

    socket.on('progress_update', (data) => {
      setProgressUpdates((prev) => [...prev, `${data.domain}: ${data.message}`]);
    });

    socket.on('preview_data', (data) => {
      data.previewData.forEach((item) => {
        const key = JSON.stringify(item);
        previewSet.current.add(key);
      });
      setPreviewData(Array.from(previewSet.current).map((str) => JSON.parse(str)));
    });

    return () => {
      socket.off('connection_rejected');
      socket.off('process_data_response');
      socket.off('refresh_response');
      socket.off('overall_progress');
      socket.off('progress_update');
      socket.off('preview_data');
    };
  }, []);

  const handleSubmit = () => {
    let parsedArray;
    try {
      parsedArray = JSON.parse(arrayInput || '[]');
    } catch {
      toast.error('Invalid JSON in array input');
      return;
    }
    setIsProcessing(true);
    socket.emit('process_data', {
      designations,
      location,
      numResults,
      arrayData: JSON.stringify(parsedArray),
      domains,
      downloadFileName,
    });
  };

  const handleRefresh = () => {
    socket.emit('refresh');
  };

  return (
    <div style={styles.container}>
      <ToastContainer position="top-right" autoClose={3000} />
      <style>{`
        @keyframes blink {
          0%   { opacity: 0.2; }
          50%  { opacity: 1; }
          100% { opacity: 0.2; }
        }
        .processing { animation: blink 1.5s linear infinite; }
      `}</style>

      <div style={styles.tabContainer}>
        <button onClick={() => setActiveTab('upload')} style={styles.tabButton(activeTab === 'upload')}>
          Upload
        </button>
        <button onClick={() => setActiveTab('preview')} style={styles.tabButton(activeTab === 'preview')}>
          Preview
        </button>
      </div>

      {activeTab === 'upload' ? (
        <div style={styles.card}>
          <div style={styles.statusRow}>
            <div style={styles.statusText}>
              {isProcessing ? <span className="processing">Processing...</span> : 'Idle State'}
            </div>
          </div>

          <h1 style={{ textAlign: 'center', marginBottom: '20px', color: '#2c3e50' }}>
            Snov Email Module
          </h1>

          <textarea
            placeholder="Enter domains here (one per line)"
            value={domains}
            onChange={(e) => setDomains(e.target.value)}
            style={styles.textarea}
            disabled={isProcessing}
          />

          <input
            type="text"
            placeholder="Comma-separated designations"
            value={designations}
            onChange={(e) => setDesignations(e.target.value)}
            style={styles.input}
            disabled={isProcessing}
          />

          <input
            type="text"
            placeholder="Comma-separated locations"
            value={location}
            onChange={(e) => setLocation(e.target.value)}
            style={styles.input}
            disabled={isProcessing}
          />

          <input
            type="text"
            placeholder="Number of emails to fetch"
            value={numResults}
            onChange={(e) => setNumResults(e.target.value)}
            style={styles.input}
            disabled={isProcessing}
          />

          <textarea
            placeholder='Enter array as JSON (e.g., {"val": "val1"})'
            value={arrayInput}
            onChange={(e) => setArrayInput(e.target.value)}
            style={{ ...styles.textarea, height: '100px' }}
            disabled={isProcessing}
          />

          <input
            type="text"
            placeholder="Enter XLSX file name (without extension)"
            value={downloadFileName}
            onChange={(e) => setDownloadFileName(e.target.value)}
            style={styles.input}
            disabled={isProcessing}
          />

          <button onClick={handleSubmit} style={styles.button('#3498db', isProcessing)} disabled={isProcessing}>
            Submit
          </button>

          <button onClick={handleRefresh} style={styles.button('#e74c3c', false)}>
            Refresh
          </button>

          <div style={styles.logContainer}>
            <h3 style={{ marginBottom: '10px' }}>Detailed Progress Updates:</h3>
            {progressUpdates.length === 0 ? (
              <p>No updates yet.</p>
            ) : (
              <ul>{progressUpdates.map((u, i) => <li key={i}>{u}</li>)}</ul>
            )}
          </div>
        </div>
      ) : (
        <div style={styles.card}>
          <h1 style={{ textAlign: 'center', marginBottom: '20px', color: '#2c3e50' }}>
            Real-Time Preview
          </h1>

          <div style={styles.progressContainer}>
            <div style={styles.progressBox}>Total Domains: {overallProgress.total}</div>
            <div style={styles.progressBox}>Processed: {overallProgress.processed}</div>
            <div style={styles.progressBox}>Remaining: {overallProgress.remaining}</div>
          </div>

          <div style={{ padding: '15px', background: '#ecf0f1', borderRadius: '6px', minHeight: '250px', marginTop: '20px' }}>
            {previewData.length > 0 ? (
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.tableHeader}>First Name</th>
                    <th style={styles.tableHeader}>Job Title</th>
                    <th style={styles.tableHeader}>Company</th>
                    <th style={styles.tableHeader}>Location</th>
                    <th style={styles.tableHeader}>Email</th>
                    <th style={styles.tableHeader}>Domain</th>
                  </tr>
                </thead>
                <tbody>
                  {previewData.map((item, idx) => (
                    <tr key={idx}>
                      <td style={styles.tableCell}>{item['First Name']}</td>
                      <td style={styles.tableCell}>{item['job title']}</td>
                      <td style={styles.tableCell}>{item['company']}</td>
                      <td style={styles.tableCell}>{item['location']}</td>
                      <td style={styles.tableCell}>{item['email']}</td>
                      <td style={styles.tableCell}>{item['domain']}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No preview data available yet.</p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default FileUpload;
