// DownloadModal.tsx
import React, { useState } from 'react';

interface DownloadModalProps {
  isOpen: boolean;
  onClose: () => void;
  onDownload: (fileName: string) => void;
  files: string[]; // Array of file names available for download
}

export default function DownloadModal({ isOpen, onClose, onDownload, files }: DownloadModalProps) {
  const [selectedFile, setSelectedFile] = useState('');

  if (!isOpen) return null;

  const handleDownload = () => {
    if (selectedFile) {
      onDownload(selectedFile);
      onClose();
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg max-w-md w-full p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Download File</h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">X</button>
        </div>
        <select
          value={selectedFile}
          onChange={(e) => setSelectedFile(e.target.value)}
          className="w-full p-2 border rounded-md mb-4"
        >
          <option value="">Select a file</option>
          {files.map((file) => (
            <option key={file} value={file}>{file}</option>
          ))}
        </select>
        <button
          onClick={handleDownload}
          className="w-full bg-blue-500 hover:bg-blue-600 text-white py-2 rounded-md"
        >
          Download
        </button>
      </div>
    </div>
  );
}
