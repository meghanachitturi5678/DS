export interface FileInfo {
  id: string;
  name: string;
  size: number;
  lastModified: string;
  content: string;
}

export interface ActiveLeases {
  [fileName: string]: boolean;
}