// LeaseManagementModal.tsx

import React from 'react';
import { FileInfo, ActiveLeases } from '../types';
import { X, Lock, Unlock } from 'lucide-react';

interface LeaseManagementModalProps {
  isOpen: boolean;
  onClose: () => void;
  files: FileInfo[];
  activeLeases: ActiveLeases;
  onLease: (fileName: string) => void;
  onUnlease: (fileName: string) => void;
}

export default function LeaseManagementModal({
  isOpen,
  onClose,
  files,
  activeLeases,
  onLease,
  onUnlease
}: LeaseManagementModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg max-w-3xl w-full p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Lease Management</h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X className="w-6 h-6" />
          </button>
        </div>

        {files.length === 0 ? (
          <p className="text-center text-gray-500">No files available for lease management.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {files.map((file) => (
                  <tr key={file.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{file.name}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      {activeLeases[file.name] ? 'Leased' : 'Available'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {activeLeases[file.name] ? (
                        <button
                          onClick={() => onUnlease(file.name)}
                          className="flex items-center text-red-600 hover:text-red-800"
                        >
                          <Unlock className="w-4 h-4 mr-1" /> Unlease
                        </button>
                      ) : (
                        <button
                          onClick={() => onLease(file.name)}
                          className="flex items-center text-green-600 hover:text-green-800"
                        >
                          <Lock className="w-4 h-4 mr-1" /> Lease
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
