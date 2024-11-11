import React from 'react';
import { LucideIcon } from 'lucide-react';

interface OperationCardProps {
  title: string;
  description: string;
  icon: LucideIcon;
  color: string;
  hoverColor: string;
  onClick: () => void;
}

export default function OperationCard({
  title,
  description,
  icon: Icon,
  color,
  hoverColor,
  onClick
}: OperationCardProps) {
  return (
    <button
      onClick={onClick}
      className={`w-full p-6 rounded-lg shadow-md ${color} ${hoverColor} transition-all duration-200 transform hover:scale-105 hover:shadow-lg`}
    >
      <div className="text-white">
        <Icon className="w-8 h-8 mb-4" />
        <h3 className="text-xl font-semibold mb-2">{title}</h3>
        <p className="text-white/90 text-sm">{description}</p>
      </div>
    </button>
  );
}