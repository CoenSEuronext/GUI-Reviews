import React, { useState } from 'react';
import { Search, ChevronDown, ChevronLeft, ChevronRight } from 'lucide-react';

const ExceptionHandling = () => {
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedFilter, setSelectedFilter] = useState('All');

  const sampleData = [
    {
      name: 'Sample Company Inc.',
      mic: 'XNYS',
      isin: 'US1234567890',
      effectiveDate: '2025-02-10',
      auditStatus: 'Pending Action',
      caType: 'ORD_DIV',
      ruleName: 'Single_Source_Ord_Div'
    },
    // Add more sample data as needed
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-blue-600 p-4">
        <h1 className="text-white text-xl font-semibold">Exception Handling</h1>
      </div>

      {/* Main Content */}
      <div className="p-4">
        {/* Filters */}
        <div className="flex gap-2 mb-4">
          <button 
            className={`px-4 py-2 rounded-full ${
              selectedFilter === 'All' 
                ? 'bg-blue-600 text-white' 
                : 'bg-gray-100'
            }`}
            onClick={() => setSelectedFilter('All')}
          >
            All
          </button>
          <button 
            className={`px-4 py-2 rounded-full ${
              selectedFilter === 'Dividend' 
                ? 'bg-blue-600 text-white' 
                : 'bg-gray-100'
            }`}
            onClick={() => setSelectedFilter('Dividend')}
          >
            Dividend
          </button>
        </div>

        {/* Search and Date Filter */}
        <div className="flex justify-between mb-4">
          <div className="flex gap-2">
            <button className="p-2 border rounded">
              <Search className="w-4 h-4" />
            </button>
            <input 
              type="date" 
              className="border rounded px-3 py-2"
              value="2025-02-09"
            />
            <input 
              type="date" 
              className="border rounded px-3 py-2"
              value="2025-02-10"
            />
          </div>
          <div className="flex gap-2">
            <select className="border rounded px-3 py-2">
              <option>All</option>
            </select>
            <div className="relative">
              <input 
                type="text" 
                placeholder="Search" 
                className="border rounded px-3 py-2 pr-8"
              />
              <Search className="w-4 h-4 absolute right-2 top-3 text-gray-400" />
            </div>
          </div>
        </div>

        {/* Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Name <ChevronDown className="w-4 h-4 inline-block" />
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  MIC
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  ISIN
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Effective Date
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Audit Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Action
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  CA Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Rule Name
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {sampleData.map((item, index) => (
                <tr key={index}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {item.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.mic}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.isin}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.effectiveDate}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="px-2 py-1 text-sm bg-blue-100 text-blue-800 rounded-full">
                      {item.auditStatus}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {/* Action buttons would go here */}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.caType}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.ruleName}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div className="flex justify-between items-center mt-4">
          <div className="flex gap-2 items-center">
            <button className="p-2 border rounded">
              <ChevronLeft className="w-4 h-4" />
            </button>
            <span className="px-3 py-2 border rounded bg-blue-600 text-white">
              1
            </span>
            <button className="p-2 border rounded">
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-500">10 / page</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ExceptionHandling;