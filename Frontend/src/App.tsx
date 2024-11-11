// App.tsx

import {
  Upload,
  Download,
  Key,
  KeyRound,
  Pencil,
  FilePlus,
  FileText,
  AlertCircle,
  Files
} from 'lucide-react';
import OperationCard from './components/OperationCard';
import Header from './components/Header';
import FileUploadModal from './components/FileUploadModal';
import FileListModal from './components/FileListModal';
import LeaseManagementModal from './components/LeaseManagementModal';
import DownloadModal from './components/DownloadModal'; // Import DownloadModal
import { useGFSOperations } from './hooks/useGFSOperations';


function App() {
  const {
    isModalOpen,
    setIsModalOpen,
    selectedOperation,
    setSelectedOperation,
    handleFileUpload,
    handleOperation,
    handleDownload,
    files,
    handleLease,
    handleUnlease,
    activeLeases,
    handleListFiles,
    storageUsed,
  } = useGFSOperations();

  const operations = [
    {
      title: 'Upload',
      description: 'Upload new files to GFS',
      icon: Upload,
      color: 'bg-blue-500',
      hoverColor: 'hover:bg-blue-600',
      onClick: () => {
        setSelectedOperation('Upload');
        setIsModalOpen(true);
      }
    },
    {
      title: 'Download',
      description: 'Download files from GFS',
      icon: Download,
      color: 'bg-green-500',
      hoverColor: 'hover:bg-green-600',
      onClick: () => {
        setSelectedOperation('Download');
        setIsModalOpen(true);
      }
    },
    {
      title: 'Lease',
      description: 'Acquire file lease',
      icon: Key,
      color: 'bg-purple-500',
      hoverColor: 'hover:bg-purple-600',
      onClick: () => {
        setSelectedOperation('Lease');
        setIsModalOpen(true);
      }
    },
    // {
    //   title: 'Unlease',
    //   description: 'Release file lease',
    //   icon: KeyRound,
    //   color: 'bg-red-500',
    //   hoverColor: 'hover:bg-red-600',
    //   onClick: () => {
    //     setSelectedOperation('Unlease');
    //     setIsModalOpen(true);
    //   }
    // },
    {
      title: 'Write',
      description: 'Write to file',
      icon: Pencil,
      color: 'bg-yellow-500',
      hoverColor: 'hover:bg-yellow-600',
      onClick: () => {
        setSelectedOperation('Write');
        setIsModalOpen(true);
      }
    },
    {
      title: 'Append',
      description: 'Append to file',
      icon: FilePlus,
      color: 'bg-indigo-500',
      hoverColor: 'hover:bg-indigo-600',
      onClick: () => {
        setSelectedOperation('Append');
        setIsModalOpen(true);
      }
    },
    {
      title: 'Read',
      description: 'Read file contents',
      icon: FileText,
      color: 'bg-teal-500',
      hoverColor: 'hover:bg-teal-600',
      onClick: () => {
        setSelectedOperation('Read');
        setIsModalOpen(true);
      }
    },
    {
      title: 'List Files',
      description: 'View all files in GFS',
      icon: Files,
      color: 'bg-gray-600',
      hoverColor: 'hover:bg-gray-700',
      onClick: () => {
        handleListFiles();
        setSelectedOperation('List Files');
      }
    }
  ];

  
  return (
    <div className="min-h-screen bg-gray-50">
      <Header />

      <main className="container mx-auto px-4 py-8">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {operations.map((op) => (
            <OperationCard
              key={op.title}
              {...op}
              // Add icon overlay to indicate lease status for leased files if necessary
            />
          ))}
        </div>

        {/* System Status Card */}
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <div className="flex items-center space-x-3">
            <AlertCircle className="text-green-500" />
            <h3 className="text-lg font-semibold">System Status</h3>
          </div>
          <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-gray-50 p-4 rounded-md">
              <p className="text-sm text-gray-600">Total Files</p>
              <p className="text-2xl font-bold text-gray-900">{files.length}</p>
            </div>
            <div className="bg-gray-50 p-4 rounded-md">
              <p className="text-sm text-gray-600">Active Leases</p>
              <p className="text-2xl font-bold text-gray-900">{Object.keys(activeLeases).length}</p>
            </div>
            <div className="bg-gray-50 p-4 rounded-md">
              <p className="text-sm text-gray-600">Storage Used</p>
              <p className="text-2xl font-bold text-gray-900">{(storageUsed / (1024 * 1024)).toFixed(2)} MB</p>
            </div>
          </div>
        </div>
      </main>

      <FileUploadModal
        isOpen={isModalOpen && selectedOperation === 'Upload'}
        onClose={() => setIsModalOpen(false)}
        operation={selectedOperation}
        onUpload={handleFileUpload}
        onOperation={handleOperation}
        files={files}
      />

      <FileListModal
        isOpen={selectedOperation === 'List Files'}
        onClose={() => setSelectedOperation('')}
        files={files}
        activeLeases={activeLeases}
      />

      <LeaseManagementModal
        isOpen={selectedOperation === 'Lease' || selectedOperation === 'Unlease'}
        onClose={() => setSelectedOperation('')}
        files={files}
        activeLeases={activeLeases}
        onLease={handleLease}
        onUnlease={handleUnlease}
      />

      <DownloadModal
        isOpen={isModalOpen && selectedOperation === 'Download'}
        onClose={() => setIsModalOpen(false)}
        files={files.map(file => file.name)}
        onDownload={handleDownload}
      />
    </div>
  );
}

export default App;