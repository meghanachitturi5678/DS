import React, { useRef, useState } from 'react';
import { X, Upload } from 'lucide-react';
import { FileInfo } from '../types';

interface FileUploadModalProps {
  isOpen: boolean;
  onClose: () => void;
  operation: string;
  onUpload: (file: File) => void;
  onOperation: (operation: string, data: any) => void;
  files: FileInfo[];
}

export default function FileUploadModal({
  isOpen,
  onClose,
  operation,
  onUpload,
  onOperation,
  files
}: FileUploadModalProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [fileName, setFileName] = useState('');
  const [fileContent, setFileContent] = useState('');

  if (!isOpen) return null;

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setSelectedFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const handleSubmit = () => {
    const data: any = {};

    // Handle specific operations and set required data
    if (operation === 'Upload' && selectedFile) {
      onUpload(selectedFile);
    } else if (['Download', 'Lease', 'Unlease', 'Read'].includes(operation)) {
      data.fileName = fileName;
    } else if (['Write', 'Append'].includes(operation)) {
      data.fileName = fileName;
      data.content = fileContent;
    }

    onOperation(operation.toLowerCase(), data);
    onClose();
    setSelectedFile(null);
    setFileName('');
    setFileContent('');
  };

  const renderContent = () => {
    switch (operation) {
      case 'Upload':
        return (
          <div
            className={`border-2 border-dashed rounded-lg p-8 text-center ${
              dragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300'
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          >
            <input
              ref={fileInputRef}
              type="file"
              className="hidden"
              onChange={handleFileSelect}
            />
            <Upload className="mx-auto h-12 w-12 text-gray-400" />
            <p className="mt-2 text-sm text-gray-600">
              Drag and drop your file here, or{' '}
              <button
                type="button"
                className="text-blue-600 hover:text-blue-700"
                onClick={() => fileInputRef.current?.click()}
              >
                browse
              </button>
            </p>
            {selectedFile && (
              <div className="mt-4 p-2 bg-gray-50 rounded">
                <p className="text-sm text-gray-700">
                  Selected: {selectedFile.name}
                </p>
              </div>
            )}
          </div>
        );

      case 'List Files':
        return (
          <div className="max-h-96 overflow-y-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Size</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {files.map((file) => (
                  <tr key={file.id}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">{file.name}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">{file.size} bytes</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );

      default:
        return (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700">File Name</label>
              <input
                type="text"
                value={fileName}
                onChange={(e) => setFileName(e.target.value)}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                placeholder="Enter file name"
              />
            </div>
            {['Write', 'Append'].includes(operation) && (
              <div>
                <label className="block text-sm font-medium text-gray-700">Content</label>
                <textarea
                  value={fileContent}
                  onChange={(e) => setFileContent(e.target.value)}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  rows={4}
                  placeholder="Enter content"
                />
              </div>
            )}
          </div>
        );
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg max-w-md w-full p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">{operation}</h2>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {renderContent()}

        <div className="mt-6 flex justify-end space-x-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-gray-700 hover:text-gray-900"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={!selectedFile && !fileName && operation !== 'List Files'}
            className={`px-4 py-2 rounded-md text-white ${
              (selectedFile || fileName || operation === 'List Files')
                ? 'bg-blue-600 hover:bg-blue-700'
                : 'bg-gray-400 cursor-not-allowed'
            }`}
          >
            {operation === 'List Files' ? 'Close' : 'Process'}
          </button>
        </div>
      </div>
    </div>
  );
}
